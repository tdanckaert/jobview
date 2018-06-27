(define-module (bash-parse)
  #:export (tokenizer))

(use-modules (ice-9 format)
	     (ice-9 ports)
	     (ice-9 regex)
	     (ice-9 q)
	     (ice-9 rdelim)
	     (ice-9 textual-ports))

;; Shell Command Language
;; http://pubs.opengroup.org/onlinepubs/9699919799/utilities/V3_chap02.html#tag_18_10

(define (reserved-word token)
  "If TOKEN corresponds to a reserved word, return the symbol for that word, otherwise #f."
  (define reserved-alist
    '(("if" . IF)
      ("then" . THEN)
      ("else" . ELSE)
      ("elif" . ELIF)
      ("fi" . FI)
      ("do" . DO)
      ("done" . DONE)
      ("case" . CASE)
      ("esac" . ESAC)
      ("while" . WHILE)
      ("until" . UNTIL)
      ("for" . FOR)
      ("in" . IN)))
  (let ((entry
	 (and (null? (cdr token)) ; ensure token consists of a single string
	      (assoc (car token) reserved-alist))))
    (and entry (cdr entry))))

(define (tokenizer port)
  (define paren-depth 0)

  (define token-count 0)

  (define expect-command? #t) ;; Is the next TOKEN a command?

  (define have-for? #f)

  ;; case-states: a stack to keep track of (nested) case ... in ... esac
  ;; constructs.
  ;;
  ;; When we enter a case ... in, we push a new EXPECT-CASE onto
  ;; case-states.
  ;;
  ;; When the top of the stack is EXPECT-CASE, and we find a "NAME)",
  ;; we change EXPECT-CASE->INSIDE-CASE.
  ;;
  ;; When we the top is INSIDE-CASE and see a DSEMI (;;), we change
  ;; INSIDE-CASE->EXPECT-CASE.
  ;;
  ;; When we are in EXPECT-CASE or INSIDE-CASE, and see "esac", we
  ;; pop one case from the stack.
  (define case-states (make-q))
  (define (inside-case?)
    (and (not (q-empty? case-states))
	 (eq? 'INSIDE-CASE (q-front case-states))))
  (define (expect-case?)
    (and (not (q-empty? case-states))
	 (eq? 'EXPECT-CASE (q-front case-states))))
  (define (set-case-state! new-state)
    (set-car! (car case-states) new-state))

  (define (return-operator op)
    (set! token-count 0)
    (case op
      ((LPAREN)
       (if (not (expect-case?))
	   (set! paren-depth (1+ paren-depth))))
      ((RPAREN)
       (if (expect-case?)
	   (begin
	     (set-case-state! 'INSIDE-CASE)
	     (set! expect-command? #t)
	     (set! token-count 0)
	     (set! have-for? #f))
	   (set! paren-depth (1- paren-depth)))
       (if (eq? paren-depth -1)
	   ;; When parsing a nested expression from a command
	   ;; substitution, paren-depth -1 means we have reached the
	   ;; closing ) for that substitution.  At the top-level,
	   ;; paren-depth -1 would indicate a syntax error...
	   (set! op 'CLOSE-SUBSTITUTION)))
      ((NEWLINE)
       (set! expect-command? #t)
       (set! token-count 0)
       (set! have-for? #f))
      ((SEMI PIPE AMPERSAND AND_IF OR_IF)
       (set! expect-command? #t))
      ((DSEMI)
       (if (inside-case?)
	   (set-case-state! 'EXPECT-CASE))
       (set! expect-command? #t)))
    op)

  (define (return-token tok)
    (set! token-count (1+ token-count))

    (let* ((reserved (reserved-word tok))
	   (command-word (and expect-command? reserved)))
      (set! expect-command? #f)
      (case command-word
	((#f) ; Not a reserved command.
	 ;; special case: check if we have CASE ... IN or FOR ... IN
	 (if (and (eq? token-count 3)
		  (or (and (expect-case?) (eq? reserved 'IN))
		      (and have-for? (or (eq? reserved 'IN)
					 (eq? reserved 'DO)))))
	     reserved
	     `(WORD ,@(reverse tok))))
	((CASE)
	 (q-push! case-states 'EXPECT-CASE)
	 'CASE)
	((ESAC)
	 (if (or (expect-case?) (inside-case?))
	     (begin (q-pop! case-states)
		    'ESAC)
	     ;; "esac" outside of a "case x in y)" context is just a word:
	     `(WORD "esac")))
	((DO)
	 (set! expect-command? #t)
	 (set! token-count 0)
	 (set! have-for? #f)
	 'DO)
	((FOR)
	 (set! have-for? #t)
	 'FOR)
	(else ; other reserved words are returned as is:
	 reserved))))

  (define (match-operator str)
    (define operators
      (make-regexp
       "(>>|>&|>\\||>|<<-|<<|<>|<&|<|;;|;|&&|&|[(]|[)]|[|][|]|[|]|\n)(.)?(.)?"))

    (define operator-alist
      '(("&&" . AND_IF)
	("||" . OR_IF)
	(";;" . DSEMI)
	(">>" . DGREAT)
	(">&" . GREATAND)
	(">|" . CLOBBER)
	("<<-" . DLESSDASH)
	("<<" . DLESS)
	("<&" . LESSAND)
	("<>" . LESSGREAT)
	("<" . LESS)
	(">" . GREATER)
	("|" . PIPE)
	("&" . AMPERSAND)
	(";" . SEMI)
	("(" . LPAREN)
	(")" . RPAREN)
	("\n" . NEWLINE)))

    (let* ((match (regexp-exec operators str))
	   (op (match:substring match 1))
	   (char1 (match:start match 2))
	   (char2 (match:start match 3)))
      (if char2
	  (unget-char port (string-ref str char2)))
      (if char1
	  (unget-char port (string-ref str char1)))
      (assoc op operator-alist)))

  (define (operator char)
    (let* ((second (get-char port))
	   (third (get-char port))
	   (candidate (if (eof-object? second)
			  (string char)
			  (if (eof-object? third)
			      (string char second)
			      (string char second third))))
	   (my-match (match-operator candidate)))
      (return-operator (cdr my-match))))
  (define (ionumber chars)
    (let ((c (get-char port)))
      (cond
       ((eof-object? c)
	(collect chars '()))
       ((char-numeric? c)
	(ionumber (cons c chars)))
       (else
	(unget-char port c)
	;; end of numeric characters.  Return an IO_NUMBER if
	;; delimited by < or >, otherwise continue parsing regular
	;; token:
	(if (or (eq? c #\<) (eq? c #\>))
	    `(IO_NUMBER ,(reverse-list->string chars))
	    (collect chars '()))))))
  (define (token-so-far chars token)
    (if (null? chars) token
	(cons (reverse-list->string chars) token)))
  (define (collect-weak-quote chars token)
    (let ((c (get-char port)))
      (case c
       ((#\\) ; \ escape sequence
	(collect-weak-quote `(,(get-char port) #\\ ,@chars) token))
       ((#\") ; end of weak quotes, return to parsing a word
	(collect (cons c chars) token))
       ((#\`) ; backquote command substitution
	(collect-weak-quote '()
			    (cons (backquote) (token-so-far chars token))))
       ((#\$) ; $ variable or $( command/arithmetic substition?
	(let ((next (get-char port)))
	  (if (eq? next #\()
	      (collect-weak-quote '()
				  (cons (substcommand) (token-so-far chars token)))
	      (begin
		(unget-char port next)
		(collect-weak-quote (cons c chars) token)))))
       (else
	(collect-weak-quote (cons c chars) token)))))
  (define (collect-strong-quote chars token)
    (let ((c (get-char port)))
      (if (eq? c #\')
	  (collect (cons c chars) token)
	  (collect-strong-quote (cons c chars) token))))
  (define (collect chars token)
    (let ((c (get-char port)))
      (if (eof-object? c)
	  (return-token (token-so-far chars token))
	  (case c
	    ((#\space #\tab #\return #\& #\( #\) #\; #\newline #\| #\< #\>)
	     ;; Tokens are delimited by whitespace or operator:
	     (unget-char port c)
	     (return-token (token-so-far chars token)))
	    ((#\\) ; \ escape sequence
	     (collect `(,(get-char port) #\\ ,@chars) token))
	    ((#\")
	     (collect-weak-quote (cons c chars) token))
	    ((#\')
	     (collect-strong-quote (cons c chars) token))
	    ((#\`)
	     (collect '()
		      (cons (backquote) (token-so-far chars token))))
	    ((#\$)
	     (let ((next (get-char port)))
	       (if (eq? next #\( )
		   (collect '()
			    (cons (substcommand) (token-so-far chars token)))
		   (begin
		     (unget-char port next)
		     (collect (cons c chars) token)))))
	    (else
	     (collect (cons c chars) token))))))
  (define (whitespace chars)
    (let ((c (get-char port)))
      (if (and (char-whitespace? c) (not (eq? c #\newline)))
	  (whitespace (cons c chars))
	  (begin
	    (unget-char port c)
	    `(WHITESPACE ,(reverse-list->string chars))))))
  (define (comment)
    (let ((line (read-line port 'peek)))
      (if (string-prefix? "PBS" line)
	  `(PBS ,(substring line 3))
	  `(COMMENT ,line))))
  (define (substcommand)
    ;; Recursively tokenize a $( command/arithmetic substitution.
    (let ((get-token (tokenizer port)))
      (let read-subst-tokens ((tokens '()))
	(let ((tok (get-token)))
	  (if (eq? tok 'CLOSE-SUBSTITUTION)
	      (cons 'SUBSTITUTE (reverse tokens))
	      (read-subst-tokens (cons tok tokens)))))))
  (define (backquote)
    ;; For backquote command substitution `...`, we just quote the
    ;; entire string until the first unquoted backquote.
    (let loop ((chars '()))
      (let ((c (get-char port)))
	(case c
	  ((#\\) ; \ escape
	   (loop `(,(get-char port) ,c ,@chars)))
	  ((#\`) ; end of `...`
	   (list 'BACKQUOTE (reverse-list->string chars)))
	  (else
	   (loop (cons c chars)))))))
  (lambda ()
    ;; TODO:
    ;;
    ;;  - handle heredoc
    ;;
    ;;  - handle bash arrays? var=(...) introduces another set of ('s...
    ;;
    ;;  - parse arithmetic expansions separately?
    ;;
    ;;  - [NAME in for] rule
    (let ((c (get-char port)))
      (if (eof-object? c)
	  '*eoi*
	  (case c
	   ((#\#) ; shell comment or PBS directive
	    (comment))
	   ((#\& #\( #\) #\; #\newline #\| #\< #\>) ; operator
	    (operator c))
	   ((#\0 #\1 #\2 #\3 #\4 #\5 #\6 #\7 #\8 #\9)
	    (ionumber (list c)))
	   ((#\")
	    (collect-weak-quote (list c) '()))
	   ((#\')
	    (collect-strong-quote (list c) '()))
	   ((#\\)
	    (collect (list (get-char port) c) '()))
	   ((#\`)
	    (collect '() (list (backquote))))
	   ((#\$)
	    (let ((next (get-char port)))
	      (if (eq? next #\( )
		  (collect '() (list (substcommand)))
		  (begin
		    (unget-char port next)
		    (collect (list c) '())))))
	   ((#\space #\tab #\return)
	    (whitespace (list c)))
	   (else
	    (collect (list c) '())))))))
