(use-modules (srfi srfi-1)
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
	     (sxml xpath))

(define filter (@ (guile) filter)) ; Name clash with "filter" from (sxml xpath).

(define +options+ (getopt-long (command-line)
			       '((help (single-char #\h) (value #f))
				 (log-file (single-char #\l) (value #t))
				 (mailto (value #t))
				 (threshold (single-char #\t) (value #t))
				 (verbose (single-char #\v) (value #f)))))

(define (show-help)
  (let* ((command (car (command-line)))
	 (last-sep (string-rindex command file-name-separator?))
	 (program-name (if last-sep
			   (substring command (1+ last-sep))
			   command)))
    (format #t "~a [options] [cluster]...

  Look for jobs on the clusters which do not reach the required
  efficiency.

  Options:

  -v, --verbose   Show external commands used.
  -l, --log-file  List of job id's which were previously detected.
  -t, --threshold Efficiency threshold (default: 10%).
  --mailto        Address(-es) to send a report to.
  -h, --help      Display this help.

" program-name)))

(if (option-ref +options+ 'help #f)
    (begin (show-help)
	   (exit 0)))

(define +log-file+ (option-ref +options+ 'log-file #f))
(define +min-effic+ (string->number (option-ref +options+ 'threshold "10")))
(define +mailto+ (option-ref +options+ 'mailto #f))
(define +verbose?+ (option-ref +options+ 'verbose #f))

(define +mailcommand+ (if +mailto+
			  (format #f "mailx -r jobview@calcua.uantwerpen.be \
-s \"low-efficiency jobs\" -S smtp=smtp.uantwerpen.be ~a" +mailto+)
			  #f))

;; The cluster on which we are running, or #f if none:
(define +host-cluster+
  (getenv "VSC_INSTITUTE_CLUSTER"))

;; The cluster(-s) we are interested in:
(define +target-clusters+
  (let ((args (option-ref +options+ '() '())))
    (if (> (length args) 0)
	args
	(list +host-cluster+))))

(unless +target-clusters+
    (show-help)
    (display "ERROR: Please specify the cluster(-s) you want to check.\n")
    (exit 1))

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

(define *node-properties*
  (let* ((nodes (fold append '() (map get-nodes +target-clusters+)))
	 (table (make-hash-table (length nodes))))
    (for-each
     (lambda (the-node)
       (sxml-match the-node
		   [(node (@ (NODEID ,id) (RCPROC ,rcproc) (RCMEM (,rcmem "0"))))
		    (hash-set! table id
			       (make-node (string->number rcproc)
					  (string->number rcmem)))])) nodes)
    table))

(define (hour-min-sec duration)
  "Convert a duration, given as a number of seconds, into a
list (hours minutes seconds)."
  (let* ((secs (modulo duration 60))
	 (mins*60 (modulo (- duration secs) 3600))
	 (hours*3600 (- duration mins*60 secs)))
    (list (/ hours*3600 3600) (/ mins*60 60) secs)))

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
						(if nodes
						    (node-procs (hash-ref *node-properties* (car nodes)))
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
      (let* ((jobs (process-output xml->sxml (checkjob-all cluster)))
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
	(map sxml->job
	     (filter running?
		     ((sxpath '(// Data job)) (list jobs child-jobs))))))
    (lambda (key cmd status message)
      (error (format #f "ERROR: Could not obtain job list: \
command '~a' returned '~a', return code ~d.\n"
		     cmd (string-trim-right message #\newline) status)))))

(define (report jobs-alist)
  (let* ((tnow (current-time))
	 (port (if +mailto+
		   (let ((result (open-output-pipe +mailcommand+)))
		     (setvbuf result 'block)
		     result)
		   (current-output-port))))
    (for-each (lambda (clusterjobs)
		(format port "~a:~%" (car clusterjobs))
		(format port "Id     User     Name ~38t Effic  Remain~%")
		(for-each
		 (lambda (job)
		   (let* ((time-remaining (- (+ (job-tstart job) (job-walltime job))
					     tnow))
			  (hh:mm:ss (format #f "~:[ ~;-~]~{~2,'0d~^:~}"
					    (< time-remaining 0)
					    (hour-min-sec (abs time-remaining)))))
		     (format port "~a ~a~t ~20@y ~38t ~5,2f ~a~%"
			     (job-id job) (job-user job) (job-name job) (job-effic job)
			     hh:mm:ss)))
		 (cdr clusterjobs))
		(format port "~%"))
	      jobs-alist)
    (if +mailto+ (close-port port))))

(let* ((tnow (current-time))
       (isbad? (lambda (job)
		 (and (< (job-effic job) +min-effic+) ; low-efficiency jobs
                      (> (- tnow (job-tstart job)) 600)))) ; which have been running for at least 10 minutes
       (badjobs (map (lambda (cluster)
		       (cons cluster
			     (filter isbad? (get-joblist cluster))))
		     +target-clusters+))
       (oldjobs (if (and +log-file+ (access? +log-file+ R_OK))
		    (with-input-from-file +log-file+ read)
		    '()))
       (newjobs (let loop ((jobs-alist badjobs)
			   (result '()))
		      (let* ((clusterjobs (car jobs-alist))
			     (tail (cdr jobs-alist))
			     (cluster (car clusterjobs))
			     (jobs (cdr clusterjobs))
			     (old (or (assoc-ref oldjobs cluster) '()))
			     (new (filter (lambda (job)
					    (not (member (job-id job) old))) jobs))
			     (result-next (if (null? new)
					      result
					      (acons cluster new result))))
			(if (null? tail)
			    result-next
			    (loop tail result-next))))))
  (unless (null? newjobs)
    (report newjobs))

  (if +log-file+
      (with-output-to-file +log-file+
	(lambda ()
	  (write (map
		  (lambda (pair)
		    (cons (car pair) (map job-id (cdr pair))))
		  badjobs))
          (display #\newline)))))
