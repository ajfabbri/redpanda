// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/future-util.h"

#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(with_timeout_abortable_test) {
    // future ready -> timeout
    {
        auto f = seastar::make_ready_future<int>(123);
        seastar::abort_source as;

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::lowres_clock::now() + 100ms, as);

        BOOST_CHECK_EQUAL(res.get(), 123);
    }

    // future ready -> timeout -> abort
    {
        auto f = seastar::sleep(50ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep(150ms).then(
          [&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::lowres_clock::now() + 100ms, as);

        abort_f.get();
        BOOST_CHECK_EQUAL(res.get(), 123);
    }

    // timeout -> future ready -> abort
    {
        auto f = seastar::sleep(150ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep(200ms).then(
          [&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::lowres_clock::now() + 100ms, as);

        abort_f.get();
        BOOST_CHECK_THROW(res.get(), seastar::timed_out_error);
    }

    // abort -> timeout -> future ready
    {
        auto wait_f = seastar::sleep(200ms);

        auto f = seastar::sleep(150ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep(50ms).then([&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::lowres_clock::now() + 100ms, as);

        abort_f.get();
        BOOST_CHECK_THROW(res.get(), seastar::sleep_aborted);
        wait_f.get();
    }

    // abort -> future ready -> timeout
    {
        auto wait_f = seastar::sleep(200ms);

        auto f = seastar::sleep(100ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep(50ms).then([&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::lowres_clock::now() + 150ms, as);

        abort_f.get();
        BOOST_CHECK_THROW(res.get(), seastar::sleep_aborted);
        wait_f.get();
    }
}
