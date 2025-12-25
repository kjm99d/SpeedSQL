/*
 * SpeedSQL - SQL Lexer
 *
 * Tokenizes SQL input for the parser
 */

#include "speedsql_internal.h"
#include <ctype.h>

/* Keyword table */
typedef struct {
    const char* word;
    token_type_t type;
} keyword_t;

static const keyword_t keywords[] = {
    {"AND", TOK_AND},
    {"AS", TOK_AS},
    {"ASC", TOK_ASC},
    {"BEGIN", TOK_BEGIN},
    {"BETWEEN", TOK_BETWEEN},
    {"BY", TOK_BY},
    {"COMMIT", TOK_COMMIT},
    {"CREATE", TOK_CREATE},
    {"DEFAULT", TOK_DEFAULT},
    {"DELETE", TOK_DELETE},
    {"DESC", TOK_DESC},
    {"DROP", TOK_DROP},
    {"FROM", TOK_FROM},
    {"GROUP", TOK_GROUP},
    {"HAVING", TOK_HAVING},
    {"IN", TOK_IN},
    {"INDEX", TOK_INDEX},
    {"INNER", TOK_INNER},
    {"INSERT", TOK_INSERT},
    {"INTO", TOK_INTO},
    {"IS", TOK_IS},
    {"JOIN", TOK_JOIN},
    {"KEY", TOK_KEY},
    {"LEFT", TOK_LEFT},
    {"LIKE", TOK_LIKE},
    {"LIMIT", TOK_LIMIT},
    {"NOT", TOK_NOT},
    {"NULL", TOK_NULL},
    {"OFFSET", TOK_OFFSET},
    {"ON", TOK_ON},
    {"OR", TOK_OR},
    {"ORDER", TOK_ORDER},
    {"OUTER", TOK_OUTER},
    {"PRIMARY", TOK_PRIMARY},
    {"RELEASE", TOK_RELEASE},
    {"RIGHT", TOK_RIGHT},
    {"ROLLBACK", TOK_ROLLBACK},
    {"SAVEPOINT", TOK_SAVEPOINT},
    {"SELECT", TOK_SELECT},
    {"SET", TOK_SET},
    {"TABLE", TOK_TABLE},
    {"TO", TOK_TO},
    {"TRANSACTION", TOK_TRANSACTION},
    {"UNIQUE", TOK_UNIQUE},
    {"UPDATE", TOK_UPDATE},
    {"VALUES", TOK_VALUES},
    {"WHERE", TOK_WHERE},
    {nullptr, TOK_EOF}
};

void lexer_init(lexer_t* lexer, const char* source) {
    lexer->start = source;
    lexer->current = source;
    lexer->line = 1;
}

static bool is_at_end(lexer_t* lexer) {
    return *lexer->current == '\0';
}

static char advance(lexer_t* lexer) {
    return *lexer->current++;
}

static char peek(lexer_t* lexer) {
    return *lexer->current;
}

static char peek_next(lexer_t* lexer) {
    if (is_at_end(lexer)) return '\0';
    return lexer->current[1];
}

static bool match(lexer_t* lexer, char expected) {
    if (is_at_end(lexer)) return false;
    if (*lexer->current != expected) return false;
    lexer->current++;
    return true;
}

static token_t make_token(lexer_t* lexer, token_type_t type) {
    token_t token;
    token.type = type;
    token.start = lexer->start;
    token.length = (int)(lexer->current - lexer->start);
    token.line = lexer->line;
    return token;
}

static token_t error_token(lexer_t* lexer, const char* message) {
    token_t token;
    token.type = TOK_ERROR;
    token.start = message;
    token.length = (int)strlen(message);
    token.line = lexer->line;
    return token;
}

static void skip_whitespace(lexer_t* lexer) {
    for (;;) {
        char c = peek(lexer);
        switch (c) {
            case ' ':
            case '\r':
            case '\t':
                advance(lexer);
                break;
            case '\n':
                lexer->line++;
                advance(lexer);
                break;
            case '-':
                if (peek_next(lexer) == '-') {
                    /* Single line comment */
                    while (peek(lexer) != '\n' && !is_at_end(lexer)) {
                        advance(lexer);
                    }
                } else {
                    return;
                }
                break;
            case '/':
                if (peek_next(lexer) == '*') {
                    /* Block comment */
                    advance(lexer);
                    advance(lexer);
                    while (!is_at_end(lexer)) {
                        if (peek(lexer) == '*' && peek_next(lexer) == '/') {
                            advance(lexer);
                            advance(lexer);
                            break;
                        }
                        if (peek(lexer) == '\n') lexer->line++;
                        advance(lexer);
                    }
                } else {
                    return;
                }
                break;
            default:
                return;
        }
    }
}

static token_type_t check_keyword(const char* start, int length) {
    for (int i = 0; keywords[i].word != nullptr; i++) {
        int klen = (int)strlen(keywords[i].word);
        if (klen == length) {
            bool match = true;
            for (int j = 0; j < length && match; j++) {
                if (toupper(start[j]) != keywords[i].word[j]) {
                    match = false;
                }
            }
            if (match) return keywords[i].type;
        }
    }
    return TOK_IDENT;
}

static token_t identifier(lexer_t* lexer) {
    while (isalnum(peek(lexer)) || peek(lexer) == '_') {
        advance(lexer);
    }

    token_type_t type = check_keyword(lexer->start,
                                       (int)(lexer->current - lexer->start));
    return make_token(lexer, type);
}

static token_t number(lexer_t* lexer) {
    bool is_float = false;

    while (isdigit(peek(lexer))) {
        advance(lexer);
    }

    /* Look for decimal part */
    if (peek(lexer) == '.' && isdigit(peek_next(lexer))) {
        is_float = true;
        advance(lexer);  /* Consume '.' */
        while (isdigit(peek(lexer))) {
            advance(lexer);
        }
    }

    /* Look for exponent */
    if (peek(lexer) == 'e' || peek(lexer) == 'E') {
        is_float = true;
        advance(lexer);
        if (peek(lexer) == '+' || peek(lexer) == '-') {
            advance(lexer);
        }
        while (isdigit(peek(lexer))) {
            advance(lexer);
        }
    }

    token_t token = make_token(lexer, is_float ? TOK_FLOAT : TOK_INTEGER);

    /* Parse value */
    if (is_float) {
        token.value.float_val = strtod(lexer->start, nullptr);
    } else {
        token.value.int_val = strtoll(lexer->start, nullptr, 10);
    }

    return token;
}

static token_t string(lexer_t* lexer, char quote) {
    while (peek(lexer) != quote && !is_at_end(lexer)) {
        if (peek(lexer) == '\n') lexer->line++;
        if (peek(lexer) == '\\' && peek_next(lexer) != '\0') {
            advance(lexer);  /* Skip escape char */
        }
        advance(lexer);
    }

    if (is_at_end(lexer)) {
        return error_token(lexer, "Unterminated string");
    }

    advance(lexer);  /* Closing quote */
    return make_token(lexer, TOK_STRING);
}

token_t lexer_next(lexer_t* lexer) {
    skip_whitespace(lexer);
    lexer->start = lexer->current;

    if (is_at_end(lexer)) return make_token(lexer, TOK_EOF);

    char c = advance(lexer);

    if (isalpha(c) || c == '_') return identifier(lexer);
    if (isdigit(c)) return number(lexer);

    switch (c) {
        case '(': return make_token(lexer, TOK_LPAREN);
        case ')': return make_token(lexer, TOK_RPAREN);
        case ',': return make_token(lexer, TOK_COMMA);
        case '.': return make_token(lexer, TOK_DOT);
        case ';': return make_token(lexer, TOK_SEMICOLON);
        case '*': return make_token(lexer, TOK_STAR);
        case '+': return make_token(lexer, TOK_PLUS);
        case '-': return make_token(lexer, TOK_MINUS);
        case '/': return make_token(lexer, TOK_SLASH);
        case '%': return make_token(lexer, TOK_PERCENT);

        case '=': return make_token(lexer, TOK_EQ);
        case '<':
            if (match(lexer, '=')) return make_token(lexer, TOK_LE);
            if (match(lexer, '>')) return make_token(lexer, TOK_NE);
            return make_token(lexer, TOK_LT);
        case '>':
            if (match(lexer, '=')) return make_token(lexer, TOK_GE);
            return make_token(lexer, TOK_GT);
        case '!':
            if (match(lexer, '=')) return make_token(lexer, TOK_NE);
            return error_token(lexer, "Expected '=' after '!'");

        case '\'': return string(lexer, '\'');
        case '"': return string(lexer, '"');

        case '?': return make_token(lexer, TOK_PARAM);
    }

    return error_token(lexer, "Unexpected character");
}

token_t lexer_peek(lexer_t* lexer) {
    /* Save state */
    const char* saved_start = lexer->start;
    const char* saved_current = lexer->current;
    int saved_line = lexer->line;

    token_t token = lexer_next(lexer);

    /* Restore state */
    lexer->start = saved_start;
    lexer->current = saved_current;
    lexer->line = saved_line;

    return token;
}
