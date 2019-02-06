(add-to-load-path (dirname (current-filename)))

(use-modules (srfi srfi-1)
	     (srfi srfi-26)
	     (ice-9 format)
	     (ice-9 getopt-long)
	     (ice-9 popen)
	     (jobtools))

(define +options+ (getopt-long (command-line)
			       '((help (single-char #\h) (value #f))
				 (log-file (single-char #\l) (value #t))
				 (mailto (single-char #\m) (value #t))
				 (threshold (single-char #\t) (value #t)))))

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

  -l, --log-file  List of job id's which were previously detected.
  -t, --threshold Efficiency threshold (default: 10%).
  -m, --mailto    Address(-es) to send a report to.
  -h, --help      Display this help.

" program-name)))

(if (option-ref +options+ 'help #f)
    (begin (show-help)
	   (exit 0)))

(define +log-file+ (option-ref +options+ 'log-file #f))
(define +min-effic+ (string->number (option-ref +options+ 'threshold "10")))
(define +mailto+ (option-ref +options+ 'mailto #f))

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
	(if +host-cluster+ (list +host-cluster+) '() ))))

(if (null? +target-clusters+)
    (begin
      (show-help)
      (display "ERROR: Please specify the cluster(-s) you want to check.\n")
      (exit 1)))

(define (hour-min-sec duration)
  "Convert a duration, given as a number of seconds, into a
list (hours minutes seconds)."
  (let* ((secs (modulo duration 60))
	 (mins*60 (modulo (- duration secs) 3600))
	 (hours*3600 (- duration mins*60 secs)))
    (list (/ hours*3600 3600) (/ mins*60 60) secs)))

(define +tnow+ (current-time))

(define (isbad? job)
  "Returns #t if we think a job is not running correctly."
  (or
   (any ; Too high load?
    (lambda (node) (> (node-load node) (* 1.2 (node-procs node))))
    (job-nodes job))
   (and ; inefficient?
    ;; Check efficiency *and* load to avoid false positives for MPI/SSH
    ;; jobs, where Torque doesn't register CPU time:
    (> (- +tnow+ (job-tstart job)) 600) ; jobs running for at least 10 minutes
    (< (job-effic job) +min-effic+)	; with low efficiency
    (any (lambda (node)		; where at least one node has low load
	   (< (node-load node) (* 0.9 (node-procs node))))
	 (job-nodes job)))))

(define (report jobs-alist)
  (let* ((port (if +mailto+
		   (let ((result (open-output-pipe +mailcommand+)))
		     (setvbuf result 'block)
		     result)
		   (current-output-port))))
    (for-each (lambda (clusterjobs)
		(let ((cluster (car clusterjobs))
		      (jobs (cdr clusterjobs)))
		  (format port "~a:~%" cluster)
		  (format port "Id     User     Name ~38t Procs  Effic  Remain   ld Min   Mdn   Max~%")
		  (for-each
		   (lambda (job)
		     (let* ((loads (if (job-nodes job)
				       (sort (map node-load (job-nodes job)) <)
				       (list 0.0)))
			    (min-load (first loads))
			    (max-load (last loads))
			    (median-load (list-ref loads (quotient (length loads) 2)))
			    (time-remaining (- (+ (job-tstart job) (job-walltime job))
					       +tnow+))
			    (hh:mm:ss (format #f "~:[ ~;-~]~{~2,'0d~^:~}"
					      (< time-remaining 0)
					      (hour-min-sec (abs time-remaining)))))
		       (format port "~a ~a~t ~20@y ~38t ~5a  ~5,2f ~a  ~5,2f ~5,2f ~5,2f~%"
			       (job-id job) (job-user job) (job-name job)
			       (job-procs job) (job-effic job)
			       hh:mm:ss
			       min-load median-load max-load)))
		   jobs))
		(format port "~%"))
	      jobs-alist)
    (if +mailto+ (close-port port))))

(let* ((badjobs (map (compose (cut filter isbad? <>) get-joblist)
		     +target-clusters+))
       (oldjobs (if (and +log-file+ (access? +log-file+ R_OK))
		    (with-input-from-file +log-file+ read)
		    '()))
       (newjobs (let loop ((jobsets badjobs)
			   (clusters +target-clusters+)
			   (result '()))
		  (if (null? jobsets)
		      result
		      (let* ((cluster (car clusters))
			     (jobs (car jobsets))
			     (old (or (assoc-ref oldjobs cluster) '()))
			     (new (filter (lambda (job)
					    (not (member (job-id job) old))) jobs)))
			(loop (cdr jobsets)
			      (cdr clusters)
			      (if (null? new)
				  result
				  (acons cluster new result))))))))

  (unless (null? newjobs)
    (report newjobs))

  (if +log-file+
      (with-output-to-file +log-file+
	(lambda ()
	  (write (map (lambda (cluster jobs)
			(cons cluster (map job-id jobs)))
		      +target-clusters+ badjobs))
          (display #\newline)))))
