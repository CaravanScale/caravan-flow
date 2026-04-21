require "./ast"

module JsonPath
  # Hand-rolled recursive-descent parser for the RFC 9535 subset this
  # shard targets. One character of lookahead, one position cursor.
  # No regex on the hot path — character checks only.
  #
  # Grammar (informal):
  #
  #   query    := '$' segment*
  #   segment  := '.' name
  #             | '.' '*'
  #             | '[' bracketed ']'
  #   bracketed := '*'                          # wildcard
  #             | quoted_string                 # name
  #             | number                        # index
  #             | slice                         # a:b[:c], any part optional
  #             | '?' '(' filter_expr ')'       # filter
  #
  #   filter_expr := or_expr
  #   or_expr     := and_expr ('||' and_expr)*
  #   and_expr    := unary ('&&' unary)*
  #   unary       := '!' unary | cmp
  #   cmp         := primary (cmp_op primary)?
  #   cmp_op      := '==' | '!=' | '<' | '<=' | '>' | '>='
  #   primary     := literal
  #               | ref
  #               | '(' or_expr ')'
  #   literal     := quoted_string | number | 'true' | 'false' | 'null'
  #   ref         := ('@' | '$') ( '.' name | '[' number ']' | '[' quoted_string ']' )*
  module Parser
    extend self

    def parse(src : String) : Query
      state = State.new(src)
      state.expect_char('$', "JSONPath query must start with '$'")
      segments = [] of Segment

      while state.more?
        ch = state.peek
        if ch == '.'
          state.advance
          if state.peek == '*'
            state.advance
            segments << Segment.wildcard
          else
            n = state.read_identifier
            raise ParseError.new("expected a name after '.'", state.pos) if n.empty?
            segments << Segment.name(n)
          end
        elsif ch == '['
          state.advance
          segments << parse_bracketed(state)
          state.expect_char(']', "expected ']' to close segment")
        else
          raise ParseError.new("unexpected character '#{ch}' in query", state.pos)
        end
      end

      Query.new(segments)
    end

    # --- segment helpers ---

    private def parse_bracketed(state : State) : Segment
      state.skip_ws
      ch = state.peek

      if ch == '*'
        state.advance
        state.skip_ws
        return Segment.wildcard
      end

      if ch == '\'' || ch == '"'
        name = state.read_quoted_string
        state.skip_ws
        return Segment.name(name)
      end

      if ch == '?'
        state.advance
        state.skip_ws
        state.expect_char('(', "expected '(' after '?'")
        expr = parse_filter_expr(state)
        state.skip_ws
        state.expect_char(')', "expected ')' to close filter")
        state.skip_ws
        return Segment.filter(expr)
      end

      # Either a single index, or a slice. Both start with an optional
      # signed integer or a colon.
      parse_index_or_slice(state)
    end

    private def parse_index_or_slice(state : State) : Segment
      first = read_optional_int(state)
      state.skip_ws

      if state.peek != ':'
        raise ParseError.new("expected index or slice", state.pos) if first.nil?
        return Segment.index(first)
      end

      # Slice form: [a:b] or [a:b:c], each of a/b/c optional.
      state.advance # consume ':'
      state.skip_ws
      second = read_optional_int(state)
      state.skip_ws
      third : Int32? = nil
      if state.peek == ':'
        state.advance
        state.skip_ws
        third = read_optional_int(state)
        state.skip_ws
      end
      Segment.slice(first, second, third)
    end

    private def read_optional_int(state : State) : Int32?
      sign = 1
      if state.peek == '-'
        sign = -1
        state.advance
      elsif state.peek == '+'
        state.advance
      end
      digits = state.read_digits
      if digits.empty?
        # backtrack the sign — it wasn't actually an integer
        # (The only time we consumed '-' without a digit following is
        # in well-formed queries impossible, but we guard anyway.)
        return nil
      end
      (digits.to_i32 * sign).to_i32
    end

    # --- filter expressions ---

    private def parse_filter_expr(state : State) : FilterExpr
      parse_or(state)
    end

    private def parse_or(state : State) : FilterExpr
      left = parse_and(state)
      loop do
        state.skip_ws
        break unless state.starts_with?("||")
        state.advance(2)
        right = parse_and(state)
        left = FilterExpr.new(FilterKind::Or, left: left, right: right)
      end
      left
    end

    private def parse_and(state : State) : FilterExpr
      left = parse_unary(state)
      loop do
        state.skip_ws
        break unless state.starts_with?("&&")
        state.advance(2)
        right = parse_unary(state)
        left = FilterExpr.new(FilterKind::And, left: left, right: right)
      end
      left
    end

    private def parse_unary(state : State) : FilterExpr
      state.skip_ws
      if state.peek == '!' && !state.starts_with?("!=")
        state.advance
        operand = parse_unary(state)
        return FilterExpr.new(FilterKind::Not, operand: operand)
      end
      parse_cmp(state)
    end

    private def parse_cmp(state : State) : FilterExpr
      left = parse_primary(state)
      state.skip_ws
      op = read_cmp_op(state)
      return left if op.nil?
      state.skip_ws
      right = parse_primary(state)
      FilterExpr.new(FilterKind::Cmp, cmp_op: op, left: left, right: right)
    end

    private def read_cmp_op(state : State) : CmpOp?
      if state.starts_with?("==")
        state.advance(2)
        return CmpOp::Eq
      end
      if state.starts_with?("!=")
        state.advance(2)
        return CmpOp::Neq
      end
      if state.starts_with?("<=")
        state.advance(2)
        return CmpOp::Lte
      end
      if state.starts_with?(">=")
        state.advance(2)
        return CmpOp::Gte
      end
      if state.peek == '<'
        state.advance
        return CmpOp::Lt
      end
      if state.peek == '>'
        state.advance
        return CmpOp::Gt
      end
      nil
    end

    private def parse_primary(state : State) : FilterExpr
      state.skip_ws
      ch = state.peek

      if ch == '('
        state.advance
        inner = parse_or(state)
        state.skip_ws
        state.expect_char(')', "expected ')' to close group")
        return inner
      end

      if ch == '\'' || ch == '"'
        s = state.read_quoted_string
        return FilterExpr.new(FilterKind::LiteralString, str_value: s)
      end

      if ch == '@' || ch == '$'
        steps = parse_ref_path(state)
        return FilterExpr.new(FilterKind::Exists, ref_path: steps)
      end

      if ch == '-' || ch == '+' || digit?(ch)
        num = read_number(state)
        return FilterExpr.new(FilterKind::LiteralNumber, num_value: num)
      end

      if state.starts_with?("true")
        state.advance(4)
        return FilterExpr.new(FilterKind::LiteralBool, bool_value: true)
      end

      if state.starts_with?("false")
        state.advance(5)
        return FilterExpr.new(FilterKind::LiteralBool, bool_value: false)
      end

      if state.starts_with?("null")
        state.advance(4)
        return FilterExpr.new(FilterKind::LiteralNull)
      end

      raise ParseError.new("unexpected character '#{ch}' in filter", state.pos)
    end

    private def parse_ref_path(state : State) : Array(PathStep)
      steps = [] of PathStep
      first = state.peek
      state.advance
      steps << (first == '@' ? PathStep.current : PathStep.root)

      loop do
        if state.peek == '.'
          state.advance
          n = state.read_identifier
          raise ParseError.new("expected a name after '.'", state.pos) if n.empty?
          steps << PathStep.name(n)
        elsif state.peek == '['
          state.advance
          state.skip_ws
          if state.peek == '\'' || state.peek == '"'
            n = state.read_quoted_string
            steps << PathStep.name(n)
          else
            i = read_optional_int(state)
            raise ParseError.new("expected an integer index", state.pos) if i.nil?
            steps << PathStep.index(i)
          end
          state.skip_ws
          state.expect_char(']', "expected ']' to close reference")
        else
          break
        end
      end

      steps
    end

    private def read_number(state : State) : Float64
      buf = String::Builder.new
      if state.peek == '-' || state.peek == '+'
        buf << state.consume
      end
      while digit?(state.peek)
        buf << state.consume
      end
      if state.peek == '.'
        buf << state.consume
        while digit?(state.peek)
          buf << state.consume
        end
      end
      if state.peek == 'e' || state.peek == 'E'
        buf << state.consume
        if state.peek == '-' || state.peek == '+'
          buf << state.consume
        end
        while digit?(state.peek)
          buf << state.consume
        end
      end
      text = buf.to_s
      text.to_f64? || raise ParseError.new("invalid number '#{text}'", state.pos)
    end

    private def digit?(ch : Char) : Bool
      ch >= '0' && ch <= '9'
    end

    # Mutable cursor into the source string. Bounds-safe: `peek`
    # returns `'\0'` past EOF rather than raising, so the parser's
    # `peek == x` checks stay clean.
    class State
      getter src : String
      getter pos : Int32

      def initialize(@src : String)
        @pos = 0
      end

      def more? : Bool
        @pos < @src.size
      end

      def peek : Char
        more? ? @src[@pos] : '\0'
      end

      def peek_at(offset : Int32) : Char
        i = @pos + offset
        i < @src.size ? @src[i] : '\0'
      end

      def advance(n : Int32 = 1) : Nil
        @pos += n
      end

      def consume : Char
        ch = peek
        @pos += 1
        ch
      end

      def starts_with?(s : String) : Bool
        return false if @pos + s.size > @src.size
        s.each_char_with_index do |ch, i|
          return false if @src[@pos + i] != ch
        end
        true
      end

      def skip_ws : Nil
        while more? && whitespace?(peek)
          @pos += 1
        end
      end

      def expect_char(ch : Char, msg : String) : Nil
        if peek != ch
          raise ParseError.new("#{msg} (got '#{peek}')", @pos)
        end
        @pos += 1
      end

      def read_identifier : String
        start = @pos
        while more?
          ch = peek
          break unless ident_char?(ch)
          @pos += 1
        end
        @src[start...@pos]
      end

      def read_digits : String
        start = @pos
        while more? && peek >= '0' && peek <= '9'
          @pos += 1
        end
        @src[start...@pos]
      end

      # Reads a single- or double-quoted string, honoring \\ \' \" \n
      # \t \r and \uXXXX escape sequences. Other escapes pass through
      # as the literal escaped character (e.g. \z -> z) — lenient to
      # match common JSONPath implementations.
      def read_quoted_string : String
        quote = peek
        unless quote == '\'' || quote == '"'
          raise ParseError.new("expected a quoted string", @pos)
        end
        @pos += 1
        buf = String::Builder.new
        while more?
          ch = peek
          if ch == quote
            @pos += 1
            return buf.to_s
          end
          if ch == '\\'
            @pos += 1
            esc = peek
            case esc
            when '\\' then buf << '\\'
            when '\'' then buf << '\''
            when '"'  then buf << '"'
            when '/'  then buf << '/'
            when 'n'  then buf << '\n'
            when 't'  then buf << '\t'
            when 'r'  then buf << '\r'
            when 'b'  then buf << '\b'
            when 'f'  then buf << '\f'
            when 'u'
              @pos += 1
              hex = @src[@pos, 4]? || raise ParseError.new("truncated \\u escape", @pos)
              raise ParseError.new("invalid \\u escape", @pos) if hex.size != 4
              code = hex.to_i?(16) || raise ParseError.new("invalid \\u escape", @pos)
              buf << code.chr
              @pos += 4
              next
            else
              buf << esc
            end
            @pos += 1
          else
            buf << ch
            @pos += 1
          end
        end
        raise ParseError.new("unterminated string literal", @pos)
      end

      private def whitespace?(ch : Char) : Bool
        ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
      end

      private def ident_char?(ch : Char) : Bool
        (ch >= 'a' && ch <= 'z') ||
          (ch >= 'A' && ch <= 'Z') ||
          (ch >= '0' && ch <= '9') ||
          ch == '_'
      end
    end
  end
end
