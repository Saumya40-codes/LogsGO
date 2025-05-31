package logql

import (
	"fmt"
	"go/token"
)

func TryAST() {
	query := "service=\"auth\" | level=error | msg=\"failed to authenticate user\""

	fset := token.NewFileSet()
	fmt.Println("Parsing query:", query)
	fmt.Println("File set created:", fset)
}
