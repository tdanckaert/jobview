(add-to-load-path (dirname (current-filename)))

(use-modules (srfi srfi-1)
	     (srfi srfi-9)
	     (srfi srfi-13)
	     (srfi srfi-26)
	     (ice-9 format)
	     (ice-9 getopt-long)
	     (ice-9 popen)
	     (jobtools))

(define +options+ (getopt-long (command-line)
			       '((help (single-char #\h) (value #f))
				 (log-file (single-char #\l) (value #t))
				 (mailto (value #t))
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
  --mailto        Address(-es) to send a report to.
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
	(list +host-cluster+))))

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

(define (report jobs-alist)
  (let* ((tnow (current-time))
	 (port (if +mailto+
		   (let ((result (open-output-pipe +mailcommand+)))
		     (setvbuf result 'block)
		     result)
		   (current-output-port))))
    (for-each (lambda (clusterjobs)
		(format port "~a:~%" (car clusterjobs))
		(format port "Id     User     Name ~38t Procs  Effic  Remain~%")
		(for-each
		 (lambda (job)
		   (let* ((time-remaining (- (+ (job-tstart job) (job-walltime job))
					     tnow))
			  (hh:mm:ss (format #f "~:[ ~;-~]~{~2,'0d~^:~}"
					    (< time-remaining 0)
					    (hour-min-sec (abs time-remaining)))))
		     (format port "~a ~a~t ~20@y ~38t ~5a  ~5,2f ~a~%"
			     (job-id job) (job-user job) (job-name job)
			     (job-procs job) (job-effic job)
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
		  (if (null? jobs-alist)
		      result
		      (let* ((clusterjobs (car jobs-alist))
			     (cluster (car clusterjobs))
			     (jobs (cdr clusterjobs))
			     (old (or (assoc-ref oldjobs cluster) '()))
			     (new (filter (lambda (job)
					    (not (member (job-id job) old))) jobs)))
			(loop (cdr jobs-alist)
			      (if (null? new)
				  result
				  (acons cluster new result))))))))

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
