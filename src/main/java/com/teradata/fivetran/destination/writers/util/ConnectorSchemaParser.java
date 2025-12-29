package com.teradata.fivetran.destination.writers.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ConnectorSchemaParser {
    private HashSet<Character> delimChars;
    private HashSet<Character> quoteChars;
    private HashMap<Character, Character> bracketChars;

    private char escapeChar = '\\';

    private boolean matchBrackets = true;
    private boolean trimSpaces = false;
    private boolean ignoreQuotes = false;
    private boolean ignoreContinousDelim = false;

    public ConnectorSchemaParser() {
        delimChars = new HashSet<Character>();
        quoteChars = new HashSet<Character>();
        bracketChars = new HashMap<Character, Character>();

        delimChars.add(',');
        delimChars.add(';');
        quoteChars.add('"');
        quoteChars.add('\'');
        bracketChars.put('(', ')');
        bracketChars.put('<', '>');
    }

    public void setDelimChar(char delimChar) {
        delimChars.clear();
        delimChars.add(delimChar);
    }

    public void addDelimChar(char delimChar) {
        delimChars.add(delimChar);
    }

    public void setQuoteChar(char quoteChar) {
        quoteChars.clear();
        quoteChars.add(quoteChar);
    }

    public void addQuoteChar(char quoteChar) {
        quoteChars.add(quoteChar);
    }

    public void setBracketChars(char bracketBeginChar, char bracketEndChar) {
        bracketChars.clear();
        bracketChars.put(bracketBeginChar, bracketEndChar);
    }

    public void addBracketChars(char bracketBeginChar, char bracketEndChar) {
        bracketChars.put(bracketBeginChar, bracketEndChar);
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public void setIgnoreQuotes(boolean ignoreQuotes) {
        this.ignoreQuotes = ignoreQuotes;
    }

    public void setMatchBrackets(boolean matchBrackets) {
        this.matchBrackets = matchBrackets;
    }

    public void setTrimSpaces(boolean trimSpaces) {
        this.trimSpaces = trimSpaces;
    }

    public void setIgnoreContinousDelim(boolean ignoreContinousDelim) {
        this.ignoreContinousDelim = ignoreContinousDelim;
    }

    public boolean isQuoted(String text) {
        return isQuoted(text, this.trimSpaces);
    }

    public boolean isQuoted(String text, boolean toTrimSpaces) {
        if (text == null || text.isEmpty()) {
            return false;
        }

        String textCopy = toTrimSpaces ? text.trim() : text;

        int currPos = 0, endPos = textCopy.length();
        char currChar = textCopy.charAt(currPos++);

        List<Character> quotes = new ArrayList<Character>();

        if (!ignoreQuotes && isQuoteChar(currChar)) {
            quotes.add(currChar);
        } else {
            return false;
        }

        int quoteCount = quotes.size();
        while (currPos < endPos) {
            currChar = textCopy.charAt(currPos++);
            if (quoteCount == 0) {
                return false;
            }

            if (!ignoreQuotes && isQuoteChar(currChar)) {
                if (currChar == quotes.get(quoteCount - 1)) {
                    quotes.remove(quoteCount - 1);
                }
                quoteCount = quotes.size();
            }
        }

        return quoteCount == 0;
    }

    public List<String> tokenize(String text) {
        return tokenize(text, 0, false);
    }

    public List<String> tokenizeKeepEscape(String text) {
        return tokenize(text, 0, true);
    }

    public List<String> tokenize(String text, int maxTokens, boolean keepEscape) {
        List<String> tokens = new ArrayList<String>();

        if (text != null && !text.isEmpty()) {
            if (maxTokens == 1) {
                tokens.add(text);
                return tokens;
            }

            int currPos = 0, endPos = text.length();
            boolean escaped = false, continousDelim = false;
            char currChar = text.charAt(currPos++);

            List<Character> quotes = new ArrayList<Character>();
            List<Character> bracketBegins = new ArrayList<Character>();

            StringBuilder builder = new StringBuilder();

            if (currChar == escapeChar) {
                if (endPos > 1) {
                    escaped = true;
                    if (keepEscape) {
                        builder.append(escapeChar);
                    }
                } else {
                    builder.append(escapeChar);
                }
            } else if (isDelimChar(currChar)) {
                tokens.add("");

                if (maxTokens == 2) {
                    tokens.add(trimSpaces ? text.substring(currPos).trim() : text.substring(currPos));
                    return tokens;
                }
            } else {
                builder.append(currChar);
                if (!ignoreQuotes && isQuoteChar(currChar)) {
                    quotes.add(currChar);
                } else if (matchBrackets && isBracketBeginChar(currChar)) {
                    bracketBegins.add(currChar);
                }
            }

            while (currPos < endPos) {
                currChar = text.charAt(currPos++);
                if (escaped) {
                    builder.append(currChar);
                    escaped = false;
                } else if (currChar == escapeChar) {
                    if (currPos == endPos) {
                        builder.append(escapeChar);
                    } else {
                        escaped = true;
                        if (keepEscape) {
                            builder.append(escapeChar);
                        }
                    }
                } else if (isDelimChar(currChar) && (tokens.size() != maxTokens - 1)) {
                    if ((!ignoreQuotes && quotes.size() > 0) || (matchBrackets && bracketBegins.size() > 0)) {
                        builder.append(currChar);
                    } else {
                        continousDelim = (builder.length() == 0);
                        if (!continousDelim) {
                            tokens.add(trimSpaces ? builder.toString().trim() : builder.toString());
                            builder.setLength(0);
                        } else if (!ignoreContinousDelim) {
                            tokens.add("");
                        }
                    }
                } else {
                    if (!ignoreQuotes && isQuoteChar(currChar)) {
                        int quoteCount = quotes.size();
                        if (quoteCount == 0) {
                            quotes.add(currChar);
                        } else if (quoteCount > 0 && currChar == quotes.get(quoteCount - 1)) {
                            quotes.remove(quoteCount - 1);
                        }
                    } else if (matchBrackets && isBracketBeginChar(currChar)) {
                        if (quotes.size() == 0) {
                            bracketBegins.add(currChar);
                        }
                    } else if (matchBrackets && isBracketEndChar(currChar) && quotes.size() == 0) {
                        int bracketBeginCount = bracketBegins.size();
                        if (bracketBeginCount > 0
                                && currChar == bracketChars.get(bracketBegins.get(bracketBeginCount - 1))) {
                            bracketBegins.remove(bracketBeginCount - 1);
                        }
                    }
                    builder.append(currChar);
                }
            }

            /*
             * Add last token
             */
            tokens.add(trimSpaces ? builder.toString().trim() : builder.toString());
        }

        return tokens;
    }

    private boolean isDelimChar(char c) {
        return delimChars.contains(c);
    }

    private boolean isQuoteChar(char c) {
        return quoteChars.contains(c);
    }

    private boolean isBracketBeginChar(char c) {
        return bracketChars.containsKey(c);
    }

    private boolean isBracketEndChar(char c) {
        return bracketChars.containsValue(c);
    }
}
