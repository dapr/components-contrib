;; This is the same logic as ../e2e-guests/rewrite/main.go, but written in
;; WebAssembly to establish baseline performance. For example, TinyGo should be
;; slower than this, but other languages are unlikley to be faster.
(module $rewrite
  ;; get_uri writes the request URI value to memory, if it isn't larger than
  ;; the buffer size limit. The result is the actual URI length in bytes.
  (import "http_handler" "get_uri" (func $get_uri
    (param $buf i32) (param $buf_limit i32)
    (result (; uri_len ;) i32)))

  ;; set_uri overwrites the request URI with one read from memory.
  (import "http_handler" "set_uri" (func $set_uri
    (param $uri i32) (param $uri_len i32)))

  ;; http-wasm guests are required to export "memory", so that imported
  ;; functions like "log" can read memory.
  (memory (export "memory") 1 (; 1 page==64KB ;))

  ;; define the URI we expect to rewrite
  (global $match_uri i32 (i32.const 0))
  (data (i32.const 0) "/v1.0/hi?name=panda")
  (global $match_uri_len i32 (i32.const 19))

  ;; define the URI we expect to rewrite
  (global $new_uri i32 (i32.const 32))
  (data (i32.const 32) "/v1.0/hello?name=teddy")
  (global $new_uri_len i32 (i32.const 22))

  ;; buf is an arbitrary area to write data.
  (global $buf i32 (i32.const 1024))

  ;; clear_buf clears any memory that may have been written.
  (func $clear_buf
    (memory.fill
      (global.get $buf)
      (global.get $match_uri_len)
      (i32.const  0)))

  ;; handle rewrites the HTTP request URI
  (func (export "handle_request") (result (; ctx_next ;) i64)

    (local $uri_len i32)

    ;; First, read the uri into memory if not larger than our limit.

    ;; uri_len = get_uri(uri, match_uri_len)
    (local.set $uri_len
      (call $get_uri (global.get $buf) (global.get $match_uri_len)))

    ;; Next, if the length read is the same as our match uri, check to see if
    ;; the characters are the same.

    ;; if uri_len != match_uri_len { next() }
    (if (i32.eq (local.get $uri_len) (global.get $match_uri_len))
      (then (if (call $memeq ;; uri == match_uri
                  (global.get $buf)
                  (global.get $match_uri)
                  (global.get $match_uri_len)) (then

        ;; Call the imported function that sets the HTTP uri.
        (call $set_uri ;; uri = new_uri
          (global.get $new_uri)
          (global.get $new_uri_len))))))

    ;; dispatch with the possibly rewritten uri.
    (call $clear_buf)
    (return (i64.const 1)))

  ;; handle_response is no-op as this is a request-only handler.
  (func (export "handle_response") (param $reqCtx i32) (param $is_error i32))

  ;; memeq is like memcmp except it returns 0 (ne) or 1 (eq)
  (func $memeq (param $ptr1 i32) (param $ptr2 i32) (param $len i32) (result i32)
    (local $i1 i32)
    (local $i2 i32)
    (local.set $i1 (local.get $ptr1)) ;; i1 := ptr1
    (local.set $i2 (local.get $ptr2)) ;; i2 := ptr1

    (loop $len_gt_zero
      ;; if mem[i1] != mem[i2]
      (if (i32.ne (i32.load8_u (local.get $i1)) (i32.load8_u (local.get $i2)))
        (then (return (i32.const 0)))) ;; return 0

      (local.set $i1  (i32.add (local.get $i1)  (i32.const 1))) ;; i1++
      (local.set $i2  (i32.add (local.get $i2)  (i32.const 1))) ;; i2++
      (local.set $len (i32.sub (local.get $len) (i32.const 1))) ;; $len--

      ;; if $len > 0 { continue } else { break }
      (br_if $len_gt_zero (i32.gt_s (local.get $len) (i32.const 0))))

    (i32.const 1)) ;; return 1
)
