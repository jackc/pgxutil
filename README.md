[![Go Reference](https://pkg.go.dev/badge/github.com/jackc/pgxutil.svg)](https://pkg.go.dev/github.com/jackc/pgxutil)
[![Build Status](https://github.com/jackc/pgxutil/actions/workflows/ci.yml/badge.svg)](https://github.com/jackc/pgxutil/actions/workflows/ci.yml)

# pgxutil

pgxutil is a collection of utilities for working with [pgx](https://github.com/jackc/pgx). They are things I personally
find useful and experiments that may eventually be incorporated into [pgx](https://github.com/jackc/pgx).

It includes higher level functions such as `Select`, `Insert`, `InsertReturning`, `Update`, and `UpdateReturning`. It also includes `*Row` variants of these functions that require exactly one row to be selected or modified.

## Package Status

The API may change at any time or the package may be abandoned. It may be better to fork or copy the code into your own
projects rather than directly depending on this package.
