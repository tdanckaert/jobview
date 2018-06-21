(add-to-load-path (dirname (current-filename)))

(use-modules (srfi srfi-1)
	     (ice-9 format)
	     (ice-9 match)
	     (ice-9 textual-ports)
	     (ncurses curses)
	     (ncurses menu)
	     (ncurses panel)
	     (bash-parse))

;(call-with-input-file "/Users/tdanckaert/Code/jobview/example1.sh"
;  (lambda (port)
;    (let ((get-token (tokenizer port)))
					;      (format #t "~y~%" (get-token)))))

(define (pretty-print win script)

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
	    ((and (eq? module-state 'EXPECT-LOAD) (equal? x "load"))
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

  (call-with-input-file script
    (lambda (port)
      (print-script (filter-spaces (tokenizer port))))))

(define stdscr (initscr))
(cbreak!)
(noecho!)
(curs-set 0)
(keypad! stdscr #t)

(if (has-colors?)
    (begin
      (start-color!)
      (use-default-colors )
      (init-pair! 1 COLOR_MAGENTA -1)))

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

(with-throw-handler #t
  (lambda ()
    (let* ((script (call-with-input-file "/Users/tdanckaert/Code/jobview/example1.sh"
		       get-string-all))
	   (ncols (getmaxx stdscr))
	   (nlines (number-of-lines script ncols))
	   (pad (newpad nlines ncols)))

      (keypad! pad #t)
      (pretty-print pad "/Users/tdanckaert/Code/jobview/example1.sh")

      (let refresh-pad ((current-line 0))
	(and (prefresh pad current-line 0 0 0 (1- (getmaxy stdscr)) (1- (getmaxx stdscr)))
	     (let process-input ((c (getch pad)))
	       (cond
		((and (or (eqv? c KEY_DOWN) (eqv? c #\d))
		      (> nlines (+ current-line (getmaxy stdscr))))
		 (refresh-pad (1+ current-line)))
		((and (or (eqv? c KEY_UP) (eqv? c #\u))
		      (> current-line 0))
		 (refresh-pad (1- current-line)))
		((not (or (eqv? c #\q) (eqv? c #\Q) (eqv? c #\space)))
		 (process-input (getch pad)))))))))
  (lambda (key . parameters)
    (endwin)))

(endwin)
