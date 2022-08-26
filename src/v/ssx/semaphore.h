/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include <utility>

namespace ssx {

// We use named semaphores because the provided name will be included in
// exception messages, making diagnosing broken or timed-out semaphores much
// easier.

template<typename Clock = seastar::timer<>::clock>
class named_semaphore
  : public seastar::
      basic_semaphore<seastar::named_semaphore_exception_factory, Clock> {
public:
    named_semaphore(size_t count, seastar::sstring name)
      : seastar::
        basic_semaphore<seastar::named_semaphore_exception_factory, Clock>(
          count, seastar::named_semaphore_exception_factory{std::move(name)}) {}
};

using semaphore = named_semaphore<>;

using semaphore_units = seastar::semaphore_units<semaphore::exception_factory>;

/*
 * A traditional mutex. If you are trying to count things or need timeouts, you
 * probably want to stick with a standard semaphore. The primary motivation for
 * this class is to formalize the mutex pattern and avoid mistakes with
 * specifying resource units which are binary with a mutex.
 *
 * Usage
 * =====
 *
 *    ```
 *    mutex m;
 *    return m.with([] { ... });
 *    ```
 *
 */
class mutex {
public:
    using duration = typename ss::semaphore::duration;
    using time_point = typename ss::semaphore::time_point;

    // TODO constructor to pass through name & change callers.
    mutex()
      : _sem(1, "mutex") {}

    template<typename Func>
    auto with(Func&& func) noexcept {
        return ss::with_semaphore(_sem, 1, std::forward<Func>(func));
    }

    template<typename Func>
    auto with(duration timeout, Func&& func) noexcept {
        return ss::with_semaphore(_sem, 1, timeout, std::forward<Func>(func));
    }

    template<typename Func>
    auto with(time_point timeout, Func&& func) noexcept {
        return ss::get_units(_sem, 1, timeout)
          .then([func = std::forward<Func>(func)](auto units) mutable {
              return ss::futurize_invoke(std::forward<Func>(func))
                .finally([units = std::move(units)] {});
          });
    }

    auto get_units() noexcept { return ss::get_units(_sem, 1); }

    auto try_get_units() noexcept { return ss::try_get_units(_sem, 1); }

    void broken() noexcept { _sem.broken(); }

    bool ready() { return _sem.waiters() == 0 && _sem.available_units() == 1; }

    size_t waiters() const noexcept { return _sem.waiters(); }

private:
    ssx::semaphore _sem;
};

} // namespace ssx
