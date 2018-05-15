(use-modules (srfi srfi-1)
	     (srfi srfi-9)
	     (srfi srfi-13)
	     (srfi srfi-26)
	     (ice-9 format)
	     (ice-9 match)
	     (ice-9 popen)
	     (ice-9 rdelim)
	     (ice-9 regex)
	     (ice-9 textual-ports)
	     (sxml simple)
	     (sxml match)
	     (sxml xpath)
	     (ncurses curses)
	     (ncurses menu)
	     (ncurses panel))

;; The cluster on which we are running, or #f if none:
(define *host-cluster*
  (getenv "VSC_INSTITUTE_CLUSTER"))

;; The cluster from which we want to obtain information:
(define *target-cluster*
  (let ((args (program-arguments)))
    (if (> (length args) 1)
	(second args)
	*host-cluster*)))

;; If no *target-cluster* is known, there is not much we can do:
(or *target-cluster*
    (error "Please specify the cluster name."))

(define (cat-jobscript jobid)
  "An external command which outputs the jobscript for JOBID on stdout."
  (format #f "ssh master-~a.uantwerpen.be sudo /bin/cat /var/spool/torque/server_priv/jobs/~a.~a.SC"
	  *target-cluster*
	  jobid
	  *target-cluster*))

(define (checkjob-all)
  "An external command which prints an XML-formatted list of all jobs
to stdout."
  (format #f "ssh login-~a.uantwerpen.be checkjob -v --xml ALL" *target-cluster*))

(define (process-output proc cmd)
  "Runs CMD as an external process, with an input port from which the
process' stdout may be read, and runs the procedure PROC that takes
this input prot as a single argument.  Throws an exception 'cmd-failed
if CMD's exit status is non-zero."
  (let ((err-port (current-error-port)))
    (set-current-error-port (%make-void-port "w")) ; Temporarily redirect stderr.
    (let* ((port (open-input-pipe cmd))
	   (result (proc port))
	   (status (close-pipe port)))
      (set-current-error-port err-port)
      (or (zero? status)
	  (throw 'cmd-failed cmd status))
      result)))

(define-record-type <node>
  (make-node procs mem)
  node?
  (procs node-procs)
  (mem node-mem))

(define *node-properties*
  (let* ((nodes ((sxpath '(// Data node))
		 (process-output
		  xml->sxml (format #f "ssh login-~a.uantwerpen.be checknode ALL --xml"
				    *target-cluster*))))
	 (table (make-hash-table (length nodes))))
    (for-each
     (lambda (the-node)
       (sxml-match the-node [(node (@ (NODEID ,id)
				      (RCPROC ,rcproc)
				      (RCMEM (,rcmem "0"))))
			     (hash-set! table id
					(make-node (string->number rcproc)
						   (string->number rcmem)))]))
     nodes)
    table))

(define (number-of-lines string)
  "Returns 1 + (number of newlines in STRING)."
  (string-fold (lambda (char count)
		 (if (eq? char #\newline) (1+ count) count))
	       1 string))

(define (hour-min-sec duration)
  "Convert a duration, given as a number of seconds, into a
list (hours minutes seconds)."
  (let* ((secs (modulo duration 60))
	 (mins*60 (modulo (- duration secs) 3600))
	 (hours*3600 (- duration mins*60 secs)))
    (list (/ hours*3600 3600) (/ mins*60 60) secs)))

(define-record-type <job>
  (make-job user id array-id name procs nodes effic tstart walltime)
  job?
  (user job-user)
  (id job-id)
  (array-id job-array-id)
  (name job-name)
  (procs job-procs)
  (nodes job-nodes)
  (effic job-effic)
  (tstart job-tstart)
  (walltime job-walltime))

(define (time-end job)
  "The time when a job must be done, based on its start time and requested walltime."
  (+ (job-tstart job) (job-walltime job)))

(define (compare field)
  "Returns a two-argument procedure that compares two job records
using the accessor FIELD, e.g. (compare job-effic)."
  (lambda (job1 job2)
    (let ((f1 (field job1))
	  (f2 (field job2)))
      ;; Take into account nil values when sorting (e.g. array-id can be #f)
      (cond
       ((nil? f1)
	(not (nil? f2))) ; f1 is nil: if f2 is not nil, f1 < f2
       ((nil? f2)
	#f) ; f2 is nil, f1 is not nil -> f1 >= f2
       ((number? f1)
	(< f1 f2))
       (else
	(string<? f1 f2))))))

(define (sxml->job x)
  "Create a job record from checkjob's xml output."
  (catch #t
    (lambda ()
      (sxml-match x [(job (@ (User ,user)
			     (JobID ,id)
			     (JobName ,name)
			     (StatPSUtl (,psutil "0"))
			     (StartTime ,tstart)
			     (AWDuration (,used-walltime "0")) ; "AWDuration" is missing for jobs that have just started
			     (ReqAWDuration ,walltime)
			     . ,job-attrs )
			  (req (@ (AllocNodeList ,alloc-nodes)
				  (TCReqMin ,min-tasks)
				  (ReqProcPerTask ,proc-per-task)
				  . ,req-attrs)))
		     (let* (;; Match ID for array sub-jobs "x[y]", or
			    ;; simply "x" for a regular job:
			    (match-id (string-match "([0-9]+)(\\[([0-9]+)\\])?" id))
			    (job-id (match:substring match-id 1))
			    (array-id (if (match:substring match-id 3)
					  (string->number (match:substring match-id 3))
					  #f))
			    (tasks (string->number (string-trim-right min-tasks #\*)))
			    (procs (* tasks (string->number proc-per-task)))
			    (psutil (string->number psutil))
			    (tstart (string->number tstart))
			    (walltime (string->number walltime))
			    (used-walltime (string->number used-walltime))
			    ;; AllocNodeList is a comma-separated list
			    ;; of nodes, each node optionally followed
			    ;; by the number of procs ":nprocs"
			    (nodes (map (lambda (s)
					  (let ((index (string-index s #\:)))
					    (if index
						(substring s 0 index)
						s)))
					    (string-split alloc-nodes #\,)))
			    (host (car nodes)))
		       (make-job user job-id array-id name procs nodes
				 ;; To calculate efficiency, we could use
				 ;; the XML entry "StatsPSDed", demanded
				 ;; processor time, but this entry is
				 ;; missing if resources were requested as
				 ;; follows: "tasks=<x>:lprocs=all".
				 (* 100 (/ psutil
					   ;; if ReqProcs is negative, this seems to mean
					   ;; the resource request was
					   ;;
					   ;; "tasks=-(ReqProcs):lprocs=all",
					   ;;
					   ;; so the actual number of requested procs is
					   ;;
					   ;; -1 * ReqProcs * (number of cores per node)
					   (* (if (> procs 0)
						  procs
						  (* -1 procs
						     (node-procs (hash-ref *node-properties* host))))
					      ;; If used-walltime is 0, round up to 1 second to avoid division by 0:
					      (max used-walltime 1))))
				 tstart walltime))]))
    (lambda (key . parameters)
      (endwin)
      (format #t "Failed to match job SXML expression~%~y
This is a bug.~%" x)
      (throw key parameters))))

(define (read-xml port)
  "Parse a series of xml documents from PORT into sxml, and return the
results as a list."
  (let ((result '()))
    (while (not (eof-object? (peek-char port)))
      (set! result (cons (xml->sxml port) result))
      (read-line port))
    result))

(define (get-joblist)
  (define (running? job-or-child)
    ;; TODO: Can this be stated more briefly?
    (equal? (sxml-match job-or-child
			[(job (@ (State ,state) . ,attrs) . ,jobdata)
			 state]
			[(child (@ (State ,state) . ,attrs) . ,childdata)
			 state]) "Running"))
  (define filter (@ (guile) filter)) ; Name clash with "filter" from (sxml xpath).
  (catch 'cmd-failed
    (lambda ()
      (let* ((jobs (process-output xml->sxml (checkjob-all)))
	     ;; "checkjob ALL" does not show array children, therefore
	     ;; we request info about all array children as well.  We
	     ;; only want to check running child jobs, otherwise the
	     ;; number of entries could be huge:
	     (array-children (filter running?
				     ((sxpath '(// Data job ArrayInfo child)) jobs)))
	     (child-jobids (sxml-match array-children
				       [(list (child (@ (Name ,jobid) . ,rest)) ...) jobid]))
	     (child-jobs (if (not (nil? child-jobids))
			     (process-output read-xml
					     ;; For efficiency, we chain all "checkjob <child-id>" commands and wrap them in one ssh command:
					     (format #f "ssh login-~a.uantwerpen.be \"~{checkjob -v --xml ~a~^; ~}\""
						     *target-cluster* child-jobids))
			     '())))
	(map sxml->job
	     (filter running?
		     ((sxpath '(// Data job)) (list jobs child-jobs))))))
    (lambda (key cmd status)
      (error (format #f "ERROR: Could not obtain job list: command '~a' returned ~d.\n" cmd status)))))

(define (get-jobscript jobid)
  (catch 'cmd-failed
    (lambda () (process-output get-string-all (cat-jobscript jobid )))
    (lambda (key cmd status)
      (format #f
	      "ERROR: Could not get script for job ~a.
command '~a' returned ~d.\n"
	      jobid cmd status))))

(define (draw-box win y x ny nx)
  "Draw a box of dimensions NY * NX, with upper left coordinates Y and X."
  (move win y x)
  (vline win (acs-vline) ny)
  (addch win (acs-ulcorner))
  (hline win (acs-hline) (- nx 2))
  (move win y (+ x (1- nx)))
  (vline win (acs-vline) ny)
  (addch win (acs-urcorner))
  (move win (+ y (1- ny)) x)
  (addch win (acs-llcorner))
  (hline win (acs-hline) (- nx 2))
  (move win (+ y (1- ny)) (+ x (1- nx)))
  (addch win (acs-lrcorner)))

(define (submitted-command jobid)
  "Return the command used to submit job with JOBID, by retrieving IWD
and SubmitArgs attributes from checkjob --xml -v."
  (let* ((jobxml (process-output xml->sxml
				 (format #f "ssh login-~a.uantwerpen.be checkjob ~a --xml -v"
					 *target-cluster* jobid)))
	 ;; Query for the child nodes of  <Job> we are interested in:
	 (query (sxpath `(// Data job))))
    (sxml-match (query jobxml)
		[(list (job (@ (IWD ,dir) (SubmitArgs ,args) . ,attrs) . ,jobrest))
		 ;; "workdir/args" should give us the file name of the submitted jobscript, possibly with some extra arguments
		 (string-append dir "$ qsub " args)])))

(define (jobscript-viewer panel %resize)
  "Return a procedure that, given a JOBID, displays that jobscript in
PANEL.  Procedure %RESIZE will be called when the terminal is resized."
  (lambda (jobid)
    (erase panel)
    (show-panel panel)

    (let ((submission (submitted-command jobid)))
      (let show-script ((script (get-jobscript jobid)))
	(let* ((nlines (number-of-lines script))
	       (pan-height (getmaxy panel))
	       (pan-width (getmaxx panel))

	       ;; TODO: check max #cols of script, and set ncols
	       ;; accordingly, or take into account the wrapping of long
	       ;; lines of the script on multiple lines of the pad
	       ;; (effectively increasing nlines).
	       (nrows (- pan-height 4))
	       (ncols (- pan-width 2))
	       (pad (newpad nlines ncols)))

	  (addstr panel submission)
	  (draw-box panel 1 0 (- pan-height 2) pan-width)
	  (move panel (1- pan-height) 1)
	  (addstr-formatted panel
			    `(b "Q Enter Space") " Go back")
	  (addstr pad script)

	  (update-panels)
	  (doupdate)

	  ;; Read input, and scroll the script inside the window if up/down is pressed.
	  (let refresh-pad ((current-line 0))
	    (prefresh pad current-line 0
		      (+ 2 (getbegy panel)) (+ 1 (getbegx panel))
		      (+ 1 (getbegy panel) nrows) (+ (getbegx panel) ncols))
	    (let process-input ((c (getch panel)))
	      (cond

	       ((and (eqv? c KEY_DOWN)
		     (> nlines (+ current-line nrows)))
		(refresh-pad (1+ current-line)))

	       ((and (eqv? c KEY_UP) (> current-line 0))
		(refresh-pad (1- current-line)))

	       ((eqv? c KEY_RESIZE)
		(%resize)
		;; resize function will reset the whole display, so run
		;; show-script again.
		(show-script script))

	       ;; Return if we get enter/space/q, otherwise read a new input character.
	       ((not (or (eqv? c #\sp)
			 (eqv? c KEY_ENTER)
			 (eqv? c #\cr)
			 (eqv? c #\nl)
			 (eqv? c #\q)
			 (eqv? c #\Q)))
		(process-input (getch panel))))))

	  ;; Clean up and return.
	  (delwin pad)
	  (hide-panel panel)
	  (update-panels)
	  (doupdate))))))

(define (ssh job)
  (endwin)
  (system (format #f "ssh -tt login-hopper.uantwerpen.be ssh ~a"
		  (car (job-nodes job))))
  (doupdate))

;; Column width, label, and formatting for the job menu:
(define *menu-table*
  `((7 "Id" "~a")
    (8 "User" "~a")
    (20 "Name" "~20@y")
    (5 "Procs" "~5a")
    (7 "Effic" "~6,2f ") ; floating point efficiency, e.g. ' 99.05'
    (9 "Remain" "~{~2,'0d~^:~} ") ; remaining time in hh:mm:ss format
    (19 "Time started" "~a")
    (5 "ArrayId" "~a")))

(define (write-menu-title win)
  (let ((y-start (getcury win)))
    (for-each
     (lambda (menu-col)
       (let ((x-start (getcurx win))
	     (width (first menu-col))
	     (title (second menu-col)))
	 ;; write first character of each colunn label in bold to
	 ;; indicate it's a key command:
	 (addch win (bold (string-ref title 0)))
	 (addstr win (substring title 1))
	 (move win y-start (+ x-start 1 width))))
     *menu-table*)))

(define (format-table-row table . data)
  (let ((format-string
	 (string-join
	  (append-map
	   (lambda (table-col) (list "~vt" (third table-col))) table) ""))
	(column-numbers
	 (reverse
	  (fold
	   (lambda (table-col previous)
	     (cons (+ 1 (first table-col) (car previous)) previous))
	   '(0) table))))
    (apply format `(#f ,format-string ,@(apply append (zip column-numbers data))))))

(define (addstr-formatted win . strings)
  "Add STRINGS to window WIN, using bold characters for all strings enclosed in (b  )."
  (match strings
    ((('b . boldstrings) . rest)
     (let ((old-attrs (attr-get win)))
       (attr-set! win A_BOLD)
       (apply addstr-formatted `(,win ,@boldstrings (attrs ,(car old-attrs))
				      ,@rest))))
    ((('attrs oldattrs) . rest)
     (attr-set! win oldattrs)
     (apply addstr-formatted `(,win ,@rest)))
    (((? xchar? c) . rest)
     (addch win c)
     (apply addstr-formatted `(,win ,@rest)))
    ((string . rest)
     (addstr win string)
     (apply addstr-formatted `(,win ,@rest)))
    (() #f)))

(define (drawmenu menu panel)

  ;; Set the main window and subwindow
  (set-menu-win! menu panel)
  (set-menu-sub! menu
		 (derwin panel
			 (- (getmaxy panel) 4)
			 (- (cols) 2) 3 1))
  (set-menu-format! menu (- (getmaxy panel) 4) 1)

  ;; Set the menu mark string
  (set-menu-mark! menu " * ")

  ;; Print a border around the main window.
  (box panel 0 0)
  (move panel 1 4)

  ;; Menu title
  (write-menu-title panel)

  (move panel 2 0)
  (addch panel (acs-ltee))
  (move panel 2 1)
  (hline panel (acs-hline) (1- (cols)))
  (move panel 2 (1- (cols)))
  (addch panel (acs-rtee))

  (post-menu menu))

(define (job->menu-item job tnow)
  (new-item
   (format #f "~5a~a" (job-id job) (if (job-array-id job) "[]" "  "))
   (format-table-row (cdr *menu-table*) ; first column 'id' is the menu item name
		     (job-user job)
		     (job-name job)
		     (job-procs job)
		     (job-effic job)
		     ;; Remaining time:
		     (hour-min-sec (- (+ (job-tstart job)
					 (job-walltime job))
				      tnow))
		     (strftime "%a %b %02d %T" (localtime (job-tstart job)))
		     (if (job-array-id job)
			 (job-array-id job) ""))))

(define (sort-up-down list less)
  "Sort LIST according to predicate LESS.  If LIST is already sorted
for this predicate, sort in the opposite direction."
  (sort list
	(if (sorted? list less) (negate less) less)))

;; We keep all generated menus in a list to work around a garbage
;; collection bug in guile-ncurses v2.2 :-/
(define menu-list '())

(define (main)

  (define stdscr (initscr))
  (define script-pan (newwin 0 0 0 0 #:panel #t))
  (define jobs-pan (newwin (1- (lines)) 0 0 0 #:panel #t))
  (define help-pan (newwin 1 0 (1- (lines)) 0 #:panel #t))

  (cbreak!)
  (noecho!)
  (curs-set 0)

  (keypad! script-pan #t)
  (keypad! jobs-pan #t)

  (move help-pan 0 4)
  (addstr-formatted help-pan
		    `(b "Q") " Quit "
		    `(b "Enter") " View Script "
		    `(b ,(acs-uarrow) ,(acs-darrow)) " Scroll "
		    `(b "S") " SSH to job master host "
		    `(b "F5") " Refresh")

  (let display-jobs ((jobs (get-joblist))
		     (sort-p (compare job-effic)))
    (let* ((jobs (sort-up-down jobs sort-p))
	   (jobs-menu (new-menu (map (cut job->menu-item <> (current-time))
				     jobs)))
	   (selected-job
	    (lambda () (list-ref jobs (item-index (current-item jobs-menu)))))
	   (update-jobs (lambda (joblist sort-p)
			  ;; We must unpost the old menu before it gets garbage collected.
			  (unpost-menu jobs-menu)
			  (display-jobs joblist sort-p)))
	   (%resize (lambda ()
		      (unpost-menu jobs-menu)
		      (resize jobs-pan (1- (lines)) (cols))
		      (resize help-pan 1 (cols))
		      (mvwin help-pan (1- (lines)) 0)
		      (drawmenu jobs-menu jobs-pan)))
	   (show-jobscript (jobscript-viewer script-pan %resize)))

      ;; Store this menu in menu-list to prevent garbage collection later on.
      (set! menu-list (cons jobs-menu menu-list))

      (drawmenu jobs-menu jobs-pan)
      (update-panels)
      (doupdate)

      ;; Main input loop.
      (let loop ((c (getch jobs-pan)))
	(cond

	 ;; Handle (Page)Up/(Page)Down keys:
	 ((eqv? c KEY_DOWN)
	  (menu-driver jobs-menu REQ_DOWN_ITEM)
	  (loop (getch jobs-pan)))

	 ((eqv? c KEY_NPAGE)
	  (menu-driver jobs-menu REQ_SCR_DPAGE)
	  (loop (getch jobs-pan)))

	 ((eqv? c KEY_UP)
	  (menu-driver jobs-menu REQ_UP_ITEM)
	  (loop (getch jobs-pan)))

	 ((eqv? c KEY_PPAGE)
	  (menu-driver jobs-menu REQ_SCR_UPAGE)
	  (loop (getch jobs-pan)))

	 ;; Sort by efficiency/procs/username/...
	 ((eqv? c #\i)
	  (update-jobs jobs (compare job-id)))

	 ((eqv? c #\u)
	  (update-jobs jobs (compare job-user)))

	 ((eqv? c #\n)
	  (update-jobs jobs (compare job-name)))

	 ((eqv? c #\p)
	  (update-jobs jobs (compare job-procs)))

	 ((eqv? c #\e)
	  (update-jobs jobs (compare job-effic)))

	 ((eqv? c #\r)
	  (update-jobs jobs (compare time-end)))

	 ((eqv? c #\t)
	  (update-jobs jobs (compare job-tstart)))

	 ((eqv? c #\a)
	  (update-jobs jobs (compare job-array-id)))

	 ;; Refresh job list.
	 ((eqv? c (key-f 5))
	  (update-jobs (get-joblist) sort-p))

	 ;; Open SSH session on the master node.
	 ((eqv? c #\s)
	  (ssh (selected-job))
	  (loop (getch jobs-pan)))

	 ;; Terminal resize events are passed as 'KEY_RESIZE'.
	 ((eqv? c KEY_RESIZE)
	  (%resize)
	  (loop (getch jobs-pan)))

	 ;; Enter or space: view jobscript.
	 ((or (eqv? c #\sp)
	      (eqv? c KEY_ENTER)
	      (eqv? c #\cr)
	      (eqv? c #\nl))
	  (show-jobscript (job-id (selected-job)))
	  (loop (getch jobs-pan)))

	 ;; If 'Q' or 'q'  is pressed, quit.  Otherwise, loop.
	 ((not (or (eqv? c #\Q) (eqv? c #\q)))
	  (loop (getch jobs-pan))))))))

;; We use a catch-all exception handler to make sure (endwin) is
;; called before quitting the program.  Otherwise, errors might leave
;; the terminal in a bad state.
(with-throw-handler #t
  main
  (lambda  (key . parameters )
    (endwin)))

(endwin)
