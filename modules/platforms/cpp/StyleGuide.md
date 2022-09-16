# C++ Style Guide for Ignite-3

The C++ style for AI-3 where possible follows the Java style for AI-3. This is to
maintain consistency across the entire source base. However for certain C++-only
features and for the stuff that might look too foreign for C++ developers we
maintain a separate set of rules described below.

## Indentation of Nested Preprocessor Directives

We use 1-space indentation after the `#` char for nested preprocessor directives.

```
#ifdef HAS_FOO
# include "foo.h"
# ifdef HAS_BAR
#  include "bar.h"
# endif
#endif
```

## Header File Include Order

Header files are included in order from more specific to less specific:

* The prototype/interface header for this implementation (ie, the .h/.hh file that
  corresponds to this .cpp/.cc file).
* Other headers from the same project, as needed.
* Headers from other non-system libraries (for example, Boost.)
* Standard C++, C, and system headers.
* Conditionally included headers.

These groups are separated by an empty line. Inside each group files are sorted in alphabetical order.

## Drop-in Replacements for Standard Library classes

Sometimes we can provide a class that that is intended as a drop-in replacement for
a class from the standard C++ library. For example we can provide a a container to
replace some standard container. In such cases it is preferred to switch from our
naming conventions (`pushBack()`) to the standard library conventions (`push_back()`).


