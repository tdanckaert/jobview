(add-to-load-path (dirname (current-filename)))

(use-modules (bash-parse)
	     (srfi srfi-1)
	     (srfi srfi-9)
	     (srfi srfi-13)
	     (srfi srfi-26)
	     (ice-9 format)
	     (ice-9 getopt-long)
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

(define +options+ (getopt-long (command-line)
			       '((help (single-char #\h) (value #f))
				 (verbose (single-char #\v) (value #f)))))

(define (show-help)
  (let* ((command (car (command-line)))
	 (last-sep (string-rindex command file-name-separator?))
	 (program-name (if last-sep
			   (substring command (1+ last-sep))
			   command)))
    (format #t "~a [options] [cluster]

  -v, --verbose  Show external commands.
  -h, --help     Display this help.

" program-name)))

(if (option-ref +options+ 'help #f)
    (begin (show-help)
	   (exit 0)))

(define +verbose?+ (option-ref +options+ 'verbose #f))

(define-record-type <job>
  (make-job user id array-id name procs nodes interactive? workdir args effic tstart walltime)
  job?
  (user job-user)
  (id job-id)
  (array-id job-array-id)
  (name job-name)
  (procs job-procs)
  (nodes job-nodes)
  (interactive? job-interactive?)
  (workdir job-workdir)
  (args job-args)
  (effic job-effic)
  (tstart job-tstart)
  (walltime job-walltime))

;; The cluster on which we are running, or #f if none:
(define +host-cluster+
  (getenv "VSC_INSTITUTE_CLUSTER"))

;; The cluster from which we want to obtain information:
(define +target-cluster+
  (let ((args (option-ref +options+ '() '())))
    (if (> (length args) 0)
	(car args)
	+host-cluster+)))

;; If no +target-cluster+ is known, there is not much we can do:
(unless +target-cluster+
    (show-help)
    (error "Please specify the cluster name."))

(define (cat-jobscript job)
  "An external command which outputs the jobscript for JOBID on stdout."
  (format #f "ssh master-~a.uantwerpen.be sudo /bin/cat /var/spool/torque/server_priv/jobs/~a.~a.SC"
	  +target-cluster+
	  (job-id job)
	  +target-cluster+))

(define (checkjob-all)
  "An external command which prints an XML-formatted list of all jobs
to stdout."
  (format #f "ssh login-~a.uantwerpen.be checkjob -v --xml ALL" +target-cluster+))

(define (process-output proc cmd)
  "Runs CMD as an external process, with an input port from which the
process' stdout may be read, and runs the procedure PROC that takes
this input prot as a single argument.  Throws an exception 'cmd-failed
if CMD's exit status is non-zero."
  (let* ((err-pipe (pipe))
	 (err-write (cdr err-pipe))
	 (err-read (car err-pipe)))
    (endwin)
    (if +verbose?+ (format #t "~a... " cmd))
    (with-error-to-port err-write
      (lambda ()
	(let* ((port (open-input-pipe cmd))
	       (ignore (setvbuf port 'block))
	       (result (proc port))
	       (status (close-pipe port)))
	  (close-port err-write)
	  (or (zero? status)
	      (throw 'cmd-failed cmd status
		     (get-string-all err-read)))
	  (if +verbose?+ (format #t "Done~%"))
	  (doupdate)
	  result)))))

(define-record-type <node>
  (make-node procs mem)
  node?
  (procs node-procs)
  (mem node-mem))

(define *node-properties*
  (let* ((nodes ((sxpath '(// Data node))
		 (process-output
		  xml->sxml (format #f "ssh login-~a.uantwerpen.be checknode --xml ALL"
				    +target-cluster+))))
	 (table (make-hash-table (length nodes))))
    (for-each
     (lambda (the-node)
       (sxml-match the-node
		   [(node (@ (NODEID ,id) (RCPROC ,rcproc) (RCMEM (,rcmem "0"))))
		    (hash-set! table id
			       (make-node (string->number rcproc)
					  (string->number rcmem)))])) nodes)
    table))

(define (number-of-lines string ncols)
  "Returns number of lines required to display STRING, when wrapping
long lines at column NCOLS, as well as at newline characters."
  (let iter ((curr-line 1)
	     (curr-col 1)
	     (curr-idx 0))
    (if (>= curr-idx (string-length string))
	curr-line
	(let ((char (string-ref string curr-idx)))
	  (cond
	   ((or (eq? char #\newline) (= curr-col ncols))
	    (iter (1+ curr-line) 0 (1+ curr-idx)))
	   (else
	    (iter curr-line (1+ curr-col) (1+ curr-idx))))))))

(define (hour-min-sec duration)
  "Convert a duration, given as a number of seconds, into a
list (hours minutes seconds)."
  (let* ((secs (modulo duration 60))
	 (mins*60 (modulo (- duration secs) 3600))
	 (hours*3600 (- duration mins*60 secs)))
    (list (/ hours*3600 3600) (/ mins*60 60) secs)))

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
			     (IWD ,dir)
			     (Flags (,flags ""))
			     (SubmitArgs (,args #f))
			     . ,job-attrs )
			  (req (@ (AllocNodeList (,alloc-nodes #f))
				  (TCReqMin ,min-tasks)
				  (ReqProcPerTask ,proc-per-task)
				  . ,req-attrs))
			  . ,rest) ; e.g. Messages
		     (let* (;; Match ID for array sub-jobs "x[y]", or
			    ;; simply "x" for a regular job:
			    (match-id (string-match "([0-9]+)(\\[([0-9]+)\\])?" id))
			    (job-id (match:substring match-id 1))
			    (array-id (if (match:substring match-id 3)
					  (string->number (match:substring match-id 3))
					  #f))
			    ;; AllocNodeList is a comma-separated list
			    ;; of nodes, each node optionally followed
			    ;; by the number of procs ":nprocs"
			    (nodes (and alloc-nodes
					;; alloc-nodes can be missing if Torque and Moab get out of sync.
					(map (lambda (s)
					       (let ((index (string-index s #\:)))
						 (if index
						     (substring s 0 index)
						     s)))
					     (string-split alloc-nodes #\,))))
			    (interactive? (member "INTERACTIVE"
						  (string-split flags #\,)))
			    (tasks (string->number (string-trim-right min-tasks #\*)))
			    (procs (* tasks (if (equal? proc-per-task "-1") ; "-1" means "all"
						(if nodes (node-procs (hash-ref *node-properties* (car nodes)))
						    0)
						(string->number proc-per-task))))
			    (psutil (string->number psutil))
			    (tstart (string->number tstart))
			    (walltime (string->number walltime))
			    (used-walltime (string->number used-walltime)))
		       (make-job user job-id array-id name procs nodes
				 interactive? dir args
				 ;; To calculate efficiency, we could use
				 ;; the XML entry "StatsPSDed", demanded
				 ;; processor time, but this entry is
				 ;; missing if resources were requested as
				 ;; follows: "tasks=<x>:lprocs=all".
				 (* 100 (/ psutil
					   (* procs (max used-walltime 1)))) ; If used-walltime is 0, round up to 1 second to avoid division by 0:
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
						     +target-cluster+ child-jobids))
			     '())))
	(map sxml->job
	     (filter running?
		     ((sxpath '(// Data job)) (list jobs child-jobs))))))
    (lambda (key cmd status message)
      (error (format #f "ERROR: Could not obtain job list: command '~a' returned '~a', return code ~d.\n" cmd message status)))))

(define (get-jobscript job)
  (catch 'cmd-failed
    (lambda () (process-output get-string-all (cat-jobscript job)))
    (lambda (key cmd status message)
      (format #f
	      "ERROR: Could not get script for job ~a.
command '~a' returned '~a', return code ~d.\n"
	      (job-id job) cmd message status))))

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

(define (job-node-loads job)
  "Get the load for each node allocated to JOB, as reported by 'mdiag
-n' or 'checknode'."
  (let* ((mdiag (format #f "ssh login-~a.uantwerpen.be \"~{mdiag -n --xml ~a~^; ~}\""
			+target-cluster+ (job-nodes job)))
	 (node-xml (process-output read-xml mdiag)))
    (sxml-match node-xml
		[(list (*TOP* (Data (node (@ (LOAD (,loads "-1")) . ,attrs)))) ...) ; LOAD attribute is sometimes missing
		 (map string->number loads)])))

(define (pretty-print win port)
  "Read a job script from PORT and print it to WIN with syntax
highlighting."

  (define (get-operator op)
    (case op
      ((LPAREN)
       "(")
      ((RPAREN)
       ")")
      ((NEWLINE)
       "\n")
      ((AMPERSAND)
       "&")
      ((AND_IF)
       "&&")
      ((OR_IF)
       "||")
      ((PIPE)
       "|")
      ((SEMI)
       ";")
      ((DSEMI)
       ";;")
      ((LESS)
       "<")
      ((LESSAND)
       "<&")
      ((DLESS)
       "<<")
      ((LESSDASH)
       "<<-")
      ((GREATER)
       ">")
      ((DGREAT)
       ">>")
      ((GREATAND)
       ">&")
      ((CLOBBER)
       ">|")
      (else
       #f)))

  (define (print token)
    (let ((old-attrs (attr-get win)))
      (match token

	('NEWLINE
	 (addstr win "\n"))

	((and x (or 'IF 'THEN 'ELSE 'ELIF 'FI 'DO 'DONE
		 'CASE 'ESAC 'WHILE 'UNTIL 'FOR 'IN))
	 (attr-on! win (logior A_BOLD (color-pair 1)))
	 (addstr win (string-downcase (symbol->string x)))
	 (attr-off! win (logior A_BOLD (color-pair 1))))

	(('COMMENT text)
	 (attr-set! win A_DIM)
	 (addstr win "#")
	 (addstr win text)
	 (attr-set! win (car old-attrs)))

	(('PBS text)
	 (attr-set! win A_DIM)
	 (addstr win "#")
	 (attr-set! win A_BOLD)
	 (addstr win "PBS")
	 (addstr win text)
	 (attr-set! win (car old-attrs)))

	(((or 'WHITESPACE 'IO_NUMBER ) text)
	 (addstr win text))

	(('WORD x ...)
	 (print x))

	(('SUBSTITUTE x ...)
	 (addstr win "$(")
	 (print-script (filter-spaces (tokenize x)))
	 (addstr win ")"))

	(('BACKQUOTE x ...)
	 (addstr win "`")
	 (print x)
	 (addstr win "`"))

	((x)
	 (print x))

	((x y ...)
	 (print x)
	 (print y))

	(x
	 (addstr win (if (string? x) x
			 (get-operator x)))))))

  (define (tokenize substitution)
    (lambda ()
      (if (null? substitution)
	  '*eoi*
	  (let ((token (car substitution)))
	    (set! substitution (cdr substitution))
	    token))))

  (define (print-script get-token)
    (define module-state #f)
    (define token-count 0)
    (let loop ((token (get-token)))
      (unless (eq? token '*eoi*)
	(if (eq? module-state 'EXPECT-MODULE-NAME)
	    (let ((old-attrs (attr-get win)))
	      (attr-set! win A_STANDOUT)
	      (print token)
	      (attr-set! win (car old-attrs)))
	    (print token))
	(match token
	  ((or 'NEWLINE 'SEMI 'PIPE 'AMPERSAND 'AND_IF 'OR_IF 'DSEMI 'RPAREN)
	   ;; We expect a new command after these operators, so reset token-count.
	   (set! token-count 0)
	   (set! module-state #f))
	  (('WORD x)
	   (cond
	    ((and (eq? token-count 0) (equal? x "module"))
	     (set! module-state 'EXPECT-LOAD))
	    ((and (eq? module-state 'EXPECT-LOAD) (or (equal? x "load") (equal? x "add")))
	     (set! module-state 'EXPECT-MODULE-NAME)))
	   (set! token-count (1+ token-count)))
	  (else
	   #f))

	(loop (get-token)))))

  (define (filter-spaces get-token)
    (lambda ()
      (let loop ((token (get-token)))
	(match token
	  (('WHITESPACE x)
	   (addstr win x)
	   (loop (get-token)))
	  (else
	   token)))))

  (print-script (filter-spaces (tokenizer port))))

(define (job-viewer panel %resize)
  "Return a procedure that, given a job, displays details about that
job in PANEL.  Procedure %RESIZE will be called when the terminal is
resized."
  (lambda (job)
    (let ((script (if (job-interactive? job)
		      "<Interactive job>"
		      (get-jobscript job)))
	  ;; Zip load with node name, and sort the pairs by increasing load:
	  (loads (and (job-nodes job)
		      (sort (zip (job-nodes job) (job-node-loads job))
			    (lambda (x y) (< (cadr x) (cadr y)))))))
      (show-panel panel)
      (let show-script ()
	(let* ((pan-height (getmaxy panel))
	       (pan-width (getmaxx panel))
	       (nrows (- pan-height 6))
	       (ncols (- pan-width 2))
	       (nlines (number-of-lines script ncols))
	       (pad (newpad nlines ncols)))
	  (erase panel)
	  (addstr-formatted panel `(b ,(job-workdir job)))
	  (and (job-args job) ; job-args is sometimes missing (jobs submitted with msub)
	       (addstr panel (string-append " $ qsub " (job-args job))))
	  (draw-box panel 1 0 3 pan-width)
	  (move panel 1 1)
	  (addstr panel "Load:")
	  (move panel 2 1)
	  (if loads
	      (let ((min-load (first loads))
		    (max-load (last loads))
		    (median-load (list-ref loads (quotient (length loads) 2))))
		(addstr-formatted panel
				  '(b "min: ") (format #f "~5,2f (~a) " (cadr min-load) (car min-load))
				  '(b "mdn: ") (format #f "~5,2f (~a) " (cadr median-load) (car median-load))
				  '(b "max: ") (format #f "~5,2f (~a) " (cadr max-load) (car max-load))))
	      (addstr-formatted panel
				`(b "*Active job has no allocated nodes*")))
	  (draw-box panel 3 0 (- pan-height 4) pan-width)
	  (move panel 3 0)
	  (addch panel (acs-ltee))
	  (addstr panel "Script:")
	  (move panel 3 (1- pan-width))
	  (addch panel (acs-rtee))
	  (move panel (1- pan-height) 1)
	  (addstr-formatted panel
			    `(b "Q Enter Space") " Go back")

	  (call-with-input-string script
			   (lambda (port)
			     (pretty-print pad port)))

	  (update-panels)
	  (doupdate)

	  ;; Read input, and scroll the script inside the window if up/down is pressed.
	  (let refresh-pad ((current-line 0))
	    (prefresh pad current-line 0
		      (+ 4 (getbegy panel)) (+ 1 (getbegx panel))
		      (+ 3 (getbegy panel) nrows) (+ (getbegx panel) ncols))
	    (let process-input ((c (getch panel)))
	      (cond

	       ((and (eqv? c KEY_DOWN)
		     (> nlines (+ current-line nrows)))
		(refresh-pad (1+ current-line)))

	       ((eqv? c KEY_NPAGE)
		;; page down: scroll down by nrows, but not beyond (nlines - nrows)
		(refresh-pad (min (- nlines nrows) (+ current-line nrows))))

	       ((and (eqv? c KEY_UP) (> current-line 0))
		(refresh-pad (1- current-line)))

	       ((eqv? c KEY_PPAGE)
		;; page up: scroll up by nrows, but not above line 0.
		(refresh-pad (max 0 (- current-line nrows))))

	       ((eqv? c KEY_RESIZE)
		(%resize)
		;; resize function will reset the whole display, so run
		;; show-script again.
		(show-script))

	       ;; Return if we get enter/space/q, otherwise read a new input character.
	       ((not (or (eqv? c #\sp)
			 (eqv? c KEY_ENTER)
			 (eqv? c #\cr)
			 (eqv? c #\nl)
			 (eqv? c #\q)
			 (eqv? c #\Q)))
		(process-input (getch panel)))))))))

    ;; Clean up and return.
    (hide-panel panel)
    (update-panels)
    (doupdate)))

(define (ssh job)
  (endwin)
  (if (job-nodes job)
      (system (format #f "ssh -tt login-hopper.uantwerpen.be ssh ~a"
		      (car (job-nodes job))))
      (begin
	(format #t "*Active job has no allocated nodes*
Press <Enter> to continue")
	(get-char (current-input-port))))
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

  (if (has-colors?)
      (begin
	(start-color!)
	(use-default-colors )
	(init-pair! 1 COLOR_MAGENTA -1)))

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
	   (show-job (job-viewer script-pan %resize)))

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

	 ;; Enter or space: view job details.
	 ((or (eqv? c #\sp)
	      (eqv? c KEY_ENTER)
	      (eqv? c #\cr)
	      (eqv? c #\nl))
	  (show-job (selected-job))
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
