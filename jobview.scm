(use-modules (srfi srfi-1)
	     (srfi srfi-9)
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
		 (make-job user id name procs host
			   (* 100 (/ (string->number psutil)
				     (string->number psdemand)))
			    tstart walltime)]))

(define joblist
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
	      (show-script script))

	     ;; Return if we get enter/space/q, otherwise read a new input character.
	     ((not (or (eqv? c #\sp)
		       (eqv? c KEY_ENTER)
		       (eqv? c #\cr)
		       (eqv? c #\nl)))
	      (process-input (getch panel))))))

	(delwin pad)))))

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

  ;; Print a border around the main window, and a title
  (box panel 0 0)
  (move panel 1 4)
  (addstr panel (format #f "Jobid ~8a ~a ~36t ~a ~a" "User" "JobName" "Procs" "Eff"))

  (move panel 2 0)
  (addch panel (acs-ltee))
  (move panel 2 1)
  (hline panel (acs-hline) (1- (cols)))
  (move panel 2 (1- (cols)))
  (addch panel (acs-rtee))

  (post-menu menu))

(define (job->menu-item job)
  (new-item (job-id job)
	    (format #f "~a ~20@y ~30t ~5a ~6,2f"
		    (job-user job)
		    (job-name job)
		    (job-procs job)
		    (job-effic job))))

(define (less-effic job1 job2)
  (<= (job-effic job1) (job-effic job2)))

(define (less-user job1 job2)
  (string>=? (job-user job1) (job-user job2)))

;; Main input loop.
(catch #t
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

      (addstr help-pan "<Q>: Quit <Enter>: View script <Up/Down>: Scroll") ;; use acs-darrow / acs uarrow?

      (let display-jobs ((jobs-menu (new-menu (map job->menu-item joblist))))
	(let* ((refresh-menu (lambda () (drawmenu jobs-menu jobs-pan)))
	       (%resize (lambda ()
			  (unpost-menu jobs-menu)
			  (resize jobs-pan (1- (lines)) (cols))
			  (resize help-pan 1 (cols))
			  (mvwin help-pan (1- (lines)) 0)
			  (refresh-menu)))
	       (show-jobscript (jobscript-viewer script-pan %resize)))

	  (refresh-menu)
	  (update-panels)
	  (doupdate)

	  ;; Process the up and down arrow keys.  Break the loop if q or Q is
	  ;; pressed.
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

	     ((eqv? c #\u)
	      (unpost-menu jobs-menu)
	      (display-jobs (new-menu (map job->menu-item
					   (sort joblist less-user)))))

	     ;; Terminal resize events are passed as 'KEY_RESIZE':
	     ((eqv? c KEY_RESIZE)
	      (%resize)
	      (loop (getch jobs-pan)))

	     ;; Enter or space: view jobscript
	     ((or (eqv? c #\sp)
		  (eqv? c KEY_ENTER)
		  (eqv? c #\cr)
		  (eqv? c #\nl))
	      (show-jobscript (item-name (current-item jobs-menu)))
	      (hide-panel script-pan)
	      (update-panels)
	      (doupdate)
	      (loop (getch jobs-pan)))

	     ;; If 'Q' or 'q'  is pressed, quit.  Otherwise, loop.
	     ((not (or (eqv? c #\Q) (eqv? c #\q)))
	      (loop (getch jobs-pan)))))))))

      (lambda  (key . parameters )
	(endwin)
	(throw key parameters)))

(endwin)
