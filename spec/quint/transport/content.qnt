// -*- mode: Bluespec; -*-
// Content from the caller to the worker and back: the task serializer and
// its arguments, the message's compression, the worker's accept_content,
// the result serializer, the reader's result settings. Does the caller
// always learn an outcome (a value or an error), and does a link run only
// after a stored SUCCESS?
//
// Celery 9c2f238, kombu 445027e. One task with a link and a link_error.
//
// * The call encodes the arguments with the task serializer; one it cannot
//   encode raises EncodeError at the call (kombu messaging.py:441-465).
// * The worker decompresses (kombu message.py:76-84) and decodes the
//   message. A compression it has no decoder for (zstd from kombu 5.7 to a
//   kombu 5.5 worker) is a decode error: logged CRITICAL and acked,
//   nothing stored (consumer.py:565-579); a content type not in
//   accept_content is an invalid task: logged and acked, nothing stored
//   (consumer.py:850-858).
// * After the body returns, the links and the next chain step are
//   published, then the result is stored; a result the result serializer
//   cannot encode makes mark_as_done raise EncodeError, and FAILURE is
//   stored with the errbacks (trace.py:599-612).
// * The reader decodes the stored result with its own result serializer
//   and result_accept_content (base.py:707-722): a reader set for json on
//   a pickle result gets DecodeError at once.
//
// Not modeled: signed messages (auth serializer), a result too large for
// the backend, chains (chain.qnt has the unencodable result's next step).
module contenttypes {
  type Phase = Idle | CallRaised | Queued | Dropped | Ran
  type Stored = Pending | Success | Failure
  type Seen = Waiting | Value | Error
  type Action =
    | Init
    | Call({ raised: bool })
    | Deliver({ dropped: bool })
    | Read({ seen: Seen })

  type Config = {
    argsEncodable: bool,     // the task serializer can encode the arguments
    compressionKnown: bool,  // the worker has a decoder for the compression
    accepted: bool,          // the content type is in the worker's accept_content
    resultEncodable: bool,   // the result serializer can encode the return value
    readerMatches: bool,     // the reader's result serializer and accept list fit
  }
  pure val ALL_OK: Config =
    { argsEncodable: true, compressionKnown: true, accepted: true,
      resultEncodable: true, readerMatches: true }
}

module content {
  import contenttypes.*

  const CFG: Config

  // ---- state ----
  var phase: Phase
  var stored: Stored
  var seen: Seen
  var links: int
  var errbacks: int
  var lastAction: Action
  var steps: int

  action init: bool = all {
    phase' = Idle, stored' = Pending, seen' = Waiting, links' = 0,
    errbacks' = 0, lastAction' = Init, steps' = 0,
  }
  action did(a: Action): bool = all { lastAction' = a, steps' = steps + 1 }

  // ---- actions ----
  // The caller publishes the task; arguments the serializer cannot encode
  // raise at the call.
  // @cite kombu kombu/messaging.py:441-465 "body) = dumps(body, serializer=serializer)"
  action call: bool = all {
    phase == Idle,
    phase' = if (CFG.argsEncodable) Queued else CallRaised,
    seen' = if (CFG.argsEncodable) seen else Error,
    stored' = stored, links' = links, errbacks' = errbacks,
    did(Call({ raised: not(CFG.argsEncodable) })),
  }

  // The worker receives the message: a decode error or a disallowed
  // content type acks it and stores nothing; otherwise the body runs, the
  // link is published, and the result is stored, or FAILURE with the
  // errbacks if it cannot be encoded.
  // @cite kombu kombu/message.py:76-84 "body = decompress(body, compression)"
  // @cite celery@9c2f238 celery/worker/consumer/consumer.py:565-579 "def on_decode_error(self, message, exc):"
  // @cite celery@9c2f238 celery/worker/consumer/consumer.py:850-858 "except (InvalidTaskError, ContentDisallowed) as exc:"
  // @cite celery@9c2f238 celery/app/trace.py:599-612 "callbacks_sent = True"
  action deliver: bool = all {
    phase == Queued,
    val dropped = not(CFG.compressionKnown) or not(CFG.accepted)
    all {
      phase' = if (dropped) Dropped else Ran,
      links' = if (dropped) links else links + 1,
      stored' = if (dropped) stored
                else if (CFG.resultEncodable) Success else Failure,
      errbacks' = if (not(dropped) and not(CFG.resultEncodable))
                    errbacks + 1 else errbacks,
      did(Deliver({ dropped: dropped })),
    },
    seen' = seen,
  }

  // The caller reads the stored result with its own result settings.
  // @cite celery@9c2f238 celery/backends/base.py:707-722 "def decode(self, payload):"
  action read: bool = all {
    seen == Waiting, stored != Pending,
    val s = if (not(CFG.readerMatches)) Error
            else if (stored == Success) Value else Error
    all { seen' = s, did(Read({ seen: s })) },
    phase' = phase, stored' = stored, links' = links, errbacks' = errbacks,
  }

  // ---- step ----
  action step: bool = any { call, deliver, read }

  // ---- properties ----------------------------------------------------------
  // invariants
  // The link runs only for a task whose SUCCESS is stored.
  val linkOnlyOnSuccess: bool = links > 0 implies stored == Success
  // A stored FAILURE is followed by its errback, and the errback runs at
  // most once.
  val errbackOnFailure: bool = (stored == Failure implies errbacks == 1)
    and errbacks <= 1

  // liveness, in three lemmas (see ../README.md). progress: finite by
  // exhaustive search with the step counter in the state.
  val quiet: bool = phase != Idle and phase != Queued
    and not(seen == Waiting and stored != Pending)
  action stutter: bool = all {
    quiet, phase' = phase, stored' = stored, seen' = seen, links' = links,
    errbacks' = errbacks, lastAction' = lastAction, steps' = steps,
  }
  action stepOrQuiet: bool = any { step, stutter }
  // At the end the caller has a value or an error; it does not wait.
  val quietDone: bool = quiet implies seen != Waiting

  // witnesses (violated means reachable)
  val neverQuiet: bool = not(quiet)
  val neverError: bool = seen != Error
}
