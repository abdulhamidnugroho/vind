package helper

import "regexp"

var IdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func IsValidIdentifier(s string) bool {
	return IdentifierRegex.MatchString(s)
}
