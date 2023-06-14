# C++ Style Guide for Ignite-3

## Namespaces

All declarations and definitions must be in the `ignite` namespace. It is allowed to have namespaces
nested to the `ignite` namespace. The technical implementation details that are not part of the public
API should be in the nested `detail` namespaces.

It is required to mark the namespace end with the comment indicating its name.

```
namespace ignite {

namespace detail {

// Implementation details, not to be used directly.

} // namespace detail

// Public interface.

} // namespace ignite
```

Namespaces do not introduce additional indentation level.

## Indentation of C++ Code

We use 4 spaces for indentation inside structs, classes, functions, nested statements, and blocks of C++ code.
TAB characters are not allowed.

```
namespace ignite::sample {

int square(int x) {
    return x * x;
}

class counter {
public:
    unsigned int inc() {
        unsignned int ret;

        {
            std::unique_lock lock(m_mutex);

            ret = m_value++;

            if (m_value == 0)
                throw std::runtime_error("counter overflow");
        }

        log("counter incremented to " + std::to_string(ret + 1));

        return ret;
    }

private:
    unsigned int m_value = 0;

    std::mutex m_mutex;
};

} // namespace ignite::sample
```

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

## Naming Conventions

We use lower case with undescore between words for almost everything:

* file names;
* type, function, and variable names.

For constants we use upper case with undescore between words. Macro constants must start with the `IGNITE_` prefix.

For private/protected class fields it is allowed but not required to use the `m_` prefix.

```
#ifndef IGNITE_FOO_LIMIT
# define IGNITE_FOO_LIMIT 42
#endif

namespace ignite::sample {

class foo_tester {
public:
    static constexpr unsigned int LIMIT = IGNITE_FOO_LIMIT;

    enum test_result {
        BELOW_LIMIT,
        AT_LIMIT,
        ABOVE_LIMIT,
    };

    test_result test_limit() {
        int value = m_counter().inc();
        return value < LIMIT ? BELOW_LIMIT : value == LIMIT ? AT_LIMIT : ABOVE_LIMIT;
    }

private:
    counter m_counter;
};

} // namespace ignite::sample
```

Template m_parameters are an exception from this rule.

```
template <typename T, typename IterT>
void foo(T a, IterT b) {
    // ...
}
```

## Header File Include Order

Header files are included in order from more specific to less specific:

* The prototype/interface header for this implementation (ie, the .h/.hh file that
  corresponds to this .cpp/.cc file).
* Other headers from the same project, as needed.
* Headers from other non-system libraries (for example, Boost.)
* Standard C++, C, and system headers.
* Conditionally included headers.

These groups are separated by an empty line. Inside each group files are sorted in alphabetical
order.

In addition to consistency this include order has one techical advantage. It's good if each header
`#include`s all the headers it depends on itself. And this order allows to detect sooner if there
is any missing dependency.
