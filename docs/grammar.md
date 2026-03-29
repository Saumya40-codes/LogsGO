## CONTEXT FREE GRAMMER in Extended Backus-Naur Form (EBNF)

---
some syntax

-> "is defined as"
AND, OR, level,.... = (terminals)
---

query      -> expr
expr       -> term (OR term)*
term       -> factor (AND factor)*
factor     -> "(" expr ")" | comparison
comparison -> ident operator value
ident      -> level | service | message
operator   -> "=" | CONTAINS
value      -> string | ident