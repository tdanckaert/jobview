(use-modules (srfi srfi-1)
	     (srfi srfi-9)
	     (srfi srfi-26)
	     (ice-9 format)
	     (ice-9 popen)
	     (ice-9 rdelim)
	     (ice-9 textual-ports)
	     (sxml simple)
	     (sxml match)
	     (sxml xpath)
	     (ncurses curses)
	     (ncurses menu)
	     (ncurses panel))

(define (read-stdout cmd)
  "Run CMD as an external process, and capture stdout.  stderr is
redirected to avoid conflicts with the curses interface."
  (let ((err-port (current-error-port)))
    (set-current-error-port (%make-void-port "w"))
    (let* ((port (open-input-pipe cmd))
	   (stdout (get-string-all port))
	   (status (close-pipe port)))
      (set-current-error-port err-port)
      (or (zero? status)
	  (throw 'cmd-failed cmd status))
      stdout)))

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
  (make-job user id name procs host effic tstart walltime)
  job?
  (user job-user)
  (id job-id)
  (name job-name)
  (procs job-procs)
  (host job-host)
  (effic job-effic)
  (tstart job-tstart)
  (walltime job-walltime))

(define (compare field)
  "Returns a two-argument procedure that compares two job records
using the accessor FIELD, e.g. (compare job-effic)."
  (lambda (job1 job2)
    (let ((f1 (field job1))
	  (f2 (field job2)))
      (if (number? f1)
	  (< f1 f2)
	  (string<? f1 f2)))))

(define (xml->job x)
  "Create a job record from showq's xml output."
  (sxml-match x [(job (@ (User ,user)
			 (JobID ,id)
			 (JobName ,name)
			 (ReqProcs ,procs)
			 (MasterHost ,host)
			 (StatPSUtl ,psutil)
			 (StatPSDed ,psdemand)
			 (StartTime ,tstart)
			 (ReqAWDuration ,walltime)
			 . ,rest ))
		 (make-job user
			   (string->number id)
			   name
			   (string->number procs)
			   host
			   (* 100 (/ (string->number psutil)
				     (string->number psdemand)))
			   (string->number tstart)
			   (string->number walltime))]))

(define (get-joblist)
  (catch 'cmd-failed
    (lambda ()
      (map xml->job ((sxpath '(// Data queue job))
		     (xml->sxml (read-stdout "ssh login-hopper.uantwerpen.be showq -r --xml")))))
    (lambda (key cmd status)
      (format (current-error-port)
	      "ERROR: Could not obtain job list.: command '~a' returned ~d.\n"
	       cmd status)
      (quit 1))))

(define (get-jobscript jobid)
  (catch 'cmd-failed
    (lambda () (read-stdout
		(format #f "ssh master-hopper.uantwerpen.be sudo /bin/cat /var/spool/torque/server_priv/jobs/~a.hopper.SC"
			jobid)))
    (lambda (key cmd status)
      (format #f
	      "ERROR: Could not get script for job ~a.
command '~a' returned ~d.\n"
	      jobid cmd status))))

(define (jobscript-viewer panel %resize)
  "Return a procedure that, given a JOBID, displays that jobscript in
PANEL.  Procedure %RESIZE will be called when the terminal is resized."
  (lambda (jobid)
    (let show-script ((script (get-jobscript jobid)))
      (erase panel)
      (show-panel panel)
      (update-panels)
      (doupdate)
      (let* ((nlines (number-of-lines script))
	     (nrows (getmaxy panel))
	     (ncols (getmaxx panel)) ; TODO: check max #cols of script, and set ncols accordingly?
	     (pad (newpad nlines ncols)))
	(addstr pad script)

	;; Read input, and scroll the script inside the window if up/down is pressed.
	(let refresh-pad ((current-line 0))
	  (prefresh pad current-line 0
		    (getbegy panel) (getbegx panel)
		    (1- nrows) (1- ncols))
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
		       (eqv? c #\nl)))
	      (process-input (getch panel))))))

	;; Clean up and return.
	(delwin pad)
	(hide-panel panel)
	(update-panels)
	(doupdate)))))

(define (ssh job)
  (endwin)
  (system (format #f "ssh -tt login-hopper.uantwerpen.be ssh ~a"
		  (job-host job)))
  (doupdate))

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
  (addstr panel (format #f "Jobid ~8a ~a ~36t ~a ~6a  ~8a  ~a"
			"User" "JobName" "Procs" "Eff" "Time" "Walltime"))

  (move panel 2 0)
  (addch panel (acs-ltee))
  (move panel 2 1)
  (hline panel (acs-hline) (1- (cols)))
  (move panel 2 (1- (cols)))
  (addch panel (acs-rtee))

  (post-menu menu))

(define (job->menu-item job tnow)
  (let ((walltime-format "~2,'0d:~2,'0d:~2,'0d"))
    (new-item (number->string (job-id job))
	      (format #f "~a ~20@y ~30t ~5a ~6,2f  ~k  ~k"
		      (job-user job)
		      (job-name job)
		      (job-procs job)
		      (job-effic job)
		      walltime-format (hour-min-sec (- tnow (job-tstart job)))
		      walltime-format (hour-min-sec (job-walltime job))))))

;; We keep all generated menus in a list to work around a garbage
;; collection bug in guile-ncurses v2.2 :-/
(define menu-list '())

(with-throw-handler #t
  ;; We use a catch-all exception handler to make sure (endwin) is
  ;; called before quitting the program.  Otherwise, errors might leave
  ;; the terminal in a bad state.
  (lambda ()

    (let* ((stdscr (initscr))
	   (script-pan (newwin 0 0 0 0 #:panel #t))
	   (jobs-pan (newwin (1- (lines)) 0 0 0 #:panel #t))
	   (help-pan (newwin 1 0 (1- (lines)) 0 #:panel #t)))

      (cbreak!)
      (noecho!)
      (curs-set 0)

      (keypad! script-pan #t)
      (keypad! jobs-pan #t)

      (addstr help-pan "<Q>: Quit <Enter>: View script <")
      (addch help-pan (acs-uarrow))
      (addch help-pan (acs-darrow))
      (addstr help-pan ">: Scroll <S>: SSH to job's master host")

      (let display-jobs ((joblist (get-joblist))
			 (sort-p (compare job-effic)))
	(let* ((jobs-menu (new-menu (map (cut job->menu-item <> (current-time))
					 (sort joblist sort-p))))
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
	     ((eqv? c #\e)
  	      (update-jobs joblist (compare job-effic)))

	     ((eqv? c #\p)
	      (update-jobs joblist (compare job-procs)))

	     ((eqv? c #\u)
  	      (update-jobs joblist (compare job-user)))

	     ((eqv? c #\t)
	      ;; We want to sort by ascending time => sort by
	      ;; descending start time => negate.
	      (update-jobs joblist (negate (compare job-tstart))))

	     ((eqv? c #\w)
	      (update-jobs joblist (compare job-walltime)))

	     ;; Refresh job list.
	     ((eqv? c #\r)
	      (update-jobs (get-joblist) sort-p))

	     ;; Open SSH session on the master node.
	     ((eqv? c #\s)
	      (ssh (list-ref (sort joblist sort-p)
			     (item-index (current-item jobs-menu))))
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
	      (show-jobscript (item-name (current-item jobs-menu)))
	      (loop (getch jobs-pan)))

	     ;; If 'Q' or 'q'  is pressed, quit.  Otherwise, loop.
	     ((not (or (eqv? c #\Q) (eqv? c #\q)))
	      (loop (getch jobs-pan)))))
	  ))))

      (lambda  (key . parameters )
	(endwin)))

(endwin)
