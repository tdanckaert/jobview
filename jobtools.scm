(define-module (jobtools)
  #:export (get-joblist
	    job-id
	    job-effic
	    job-user
	    job-name
	    job-tstart
	    job-walltime))

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
	     (sxml xpath))

(define filter (@ (guile) filter)) ; Name clash with "filter" from (sxml xpath).

(define (checkjob-all cluster)
  "An external command which prints an XML-formatted list of all jobs
to stdout."
  (format #f "ssh login-~a.uantwerpen.be checkjob -v --xml ALL" cluster))

(define (process-output proc cmd)
  "Runs CMD as an external process, with an input port from which the
process' stdout may be read, and runs the procedure PROC that takes
this input prot as a single argument.  Throws an exception 'cmd-failed
if CMD's exit status is non-zero."
  (let* ((err-pipe (pipe))
	 (err-write (cdr err-pipe))
	 (err-read (car err-pipe)))
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
	  result)))))

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

(define-record-type <node>
  (make-node procs mem)
  node?
  (procs node-procs)
  (mem node-mem))

(define (get-nodes cluster)
  (let ((checknode (format #f
			   "ssh login-~a.uantwerpen.be checknode --xml ALL"
			   cluster)))
    ((sxpath '(// Data node)) (process-output xml->sxml checknode))))

(define (node-properties clusters)
  (let* ((nodes (fold append '() (map get-nodes clusters)))
	 (table (make-hash-table (length nodes))))
    (for-each
     (lambda (the-node)
       (sxml-match the-node
		   [(node (@ (NODEID ,id) (RCPROC ,rcproc) (RCMEM (,rcmem "0"))))
		    (hash-set! table id
			       (make-node (string->number rcproc)
					  (string->number rcmem)))])) nodes)
    table))

(define (sxml->job x node-properties)
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
						(if nodes
						    (node-procs (hash-ref node-properties (car nodes)))
						    0)
						(string->number proc-per-task))))
			    (psutil (string->number psutil))
			    (tstart (string->number tstart))
			    (walltime (string->number walltime))
			    (used-walltime (string->number used-walltime))
			    ;; To calculate efficiency, we could
			    ;; use the XML entry "StatsPSDed",
			    ;; (reserved CPU-time), but this
			    ;; entry is missing if resources were
			    ;; requested as follows:
			    ;; "tasks=<x>:lprocs=all".
			    ;; Therefore, we calculate the
			    ;; reserved CPU-time from walltime
			    ;; and number of procs.
			    (effic (if (> procs 0) ; procs may be 0 if alloc-nodes is missing
				       (* 100 (/ psutil
						 (* procs
						    (max used-walltime 1)))) ; If used-walltime is 0, round up to 1 second to avoid division by 0
				       -1))) ; if we can't calculate efficiency
		       (make-job user job-id array-id name procs nodes
				 interactive? dir args effic tstart walltime))]))
    (lambda (key . parameters)
      (format #t "Failed to match job SXML expression~%~y
This is a bug.~%" x)
      (throw key parameters))))

(define (read-xml port)
  "Parse a series of newline-separated xml documents from PORT into
sxml, and return the results as a list."
  (reverse
   (let loop ((result '()))
     (if (eof-object? (peek-char port))
	 result
	 (let ((record (xml->sxml port)))
	   (read-line port)
	   (loop (cons record result)))))))

(define (get-joblist cluster)
  (define (running? job-or-child)
    ;; TODO: Can this be stated more briefly?
    (equal? (sxml-match job-or-child
			[(job (@ (State ,state) . ,attrs) . ,jobdata)
			 state]
			[(child (@ (State ,state) . ,attrs) . ,childdata)
			 state]) "Running"))
  (catch 'cmd-failed
    (lambda ()
      (let* ((nodes (node-properties (list cluster)))
	     (jobs (process-output xml->sxml (checkjob-all cluster)))
	     ;; "checkjob ALL" does not show array children, therefore
	     ;; we request info about all array children as well.  We
	     ;; only want to check running child jobs, otherwise the
	     ;; number of entries could be huge:
	     (array-children (filter running?
				     ((sxpath '(// Data job ArrayInfo child)) jobs)))
	     (child-jobids (sxml-match array-children
				       [(list (child (@ (Name ,jobid) . ,rest)) ...) jobid]))
	     (child-jobs
	      (if (null? child-jobids)
		  '()
		  (begin
		    (when (> (length child-jobids) 10)
		      ;; Running checkjob for each child-jobid can
		      ;; take some time.
		      (format #t "Retrieving ~d array jobs.~%" (length child-jobids)))
		    (process-output
		     read-xml
		     ;; For efficiency, we chain all "checkjob <child-id>" commands and wrap them in one ssh command:
		     (format #f "ssh login-~a.uantwerpen.be \"~{checkjob -v --xml ~a~^; ~}\""
			     cluster child-jobids))))))
	(map (lambda (sxml) (sxml->job sxml nodes))
	     (filter running?
		     ((sxpath '(// Data job)) (list jobs child-jobs))))))
    (lambda (key cmd status message)
      (error (format #f "ERROR: Could not obtain job list: \
command '~a' returned '~a', return code ~d.\n"
		     cmd (string-trim-right message #\newline) status)))))
