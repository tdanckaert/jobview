(add-to-load-path (dirname (current-filename)))

(use-modules (srfi srfi-1)
	     (srfi srfi-9)
	     (srfi srfi-13)
	     (srfi srfi-26)
	     (ice-9 format)
	     (ice-9 getopt-long)
	     (ice-9 match)
	     (ice-9 textual-ports)
	     (ncurses curses)
	     (ncurses menu)
	     (ncurses panel)
	     (bash-parse)
	     (jobtools))

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
    (display "ERROR: Please specify the cluster name.\n")
    (exit 1))

;; Wrap some external commands from jobtools in (endwin)...(doupdate)
;; to avoid messing up terminal state when those commands print
;; output:
(define (my-get-joblist)
  (endwin)
  (let ((result (get-joblist +target-cluster+)))
    (doupdate)
    result))

(define (my-get-jobscript job)
  (endwin)
  (let ((result (get-jobscript job +target-cluster+)))
    (doupdate)
    result))

(define (my-node-loads job)
  (endwin)
  (let ((result (job-node-loads job +target-cluster+)))
    (doupdate)
    result))

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

(define (pretty-print win port)
  "Read a job script from PORT and print it to WIN with syntax
highlighting."

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
			 (operator->string x)))))))

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
		      (my-get-jobscript job)))
	  ;; Zip load with node name, and sort the pairs by increasing load:
	  (loads (and (job-nodes job)
		      (sort (zip (job-nodes job) (my-node-loads job))
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
  `((8 "Id" "~a")
    (8 "User" "~a")
    (20 "Name" "~20@y")
    (5 "Procs" "~5a")
    (7 " Effic" "~6,2f ") ; floating point efficiency, e.g. ' 99.05'
    (10 " Remain" "~a")
    (19 "Time started" "~a")
    (5 "ArrayId" "~a")))

(define (write-menu-title win)
  (let ((y-start (getcury win)))
    (for-each
     (lambda (menu-col)
       (let* ((x-start (getcurx win))
	      (width (first menu-col))
	      (title (second menu-col))
	      (spacing (string-skip title char-whitespace?)))
	 ;; write first non-whitespace character of each column label in bold to
	 ;; indicate it's a key command:
	 (addstr win (substring title 0 spacing)) ; leading spaces
	 (addch win (bold (string-ref title spacing))) ; first non-whitespace char
	 (addstr win (substring title (1+ spacing))) ; remaining characters
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
  (let* ((name (format #f "~5a~:[  ~;[]~]" (job-id job) (job-array-id job)))
	 (time-remaining (- (+ (job-tstart job) (job-walltime job))
			    tnow))
	 (hh:mm:ss (format #f "~:[ ~;-~]~{~2,'0d~^:~}"
			   (< time-remaining 0)
			   (hour-min-sec (abs time-remaining))))
	 (label (format-table-row (cdr *menu-table*) ; first column 'id' is the menu item name
			   (job-user job)
			   (job-name job)
			   (job-procs job)
			   (job-effic job)
			   hh:mm:ss
			   (strftime "%a %b %02d %T" (localtime (job-tstart job)))
			   (if (job-array-id job)
			       (job-array-id job) ""))))
    (new-item name label)))

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

  (let display-jobs ((jobs (my-get-joblist))
		     (sort-p (compare job-effic)))
    (let* ((jobs (sort-up-down jobs sort-p))
	   (jobs-menu
	    (new-menu (map (cut job->menu-item <> (current-time)) jobs)))
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
	  (update-jobs (my-get-joblist) sort-p))

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
