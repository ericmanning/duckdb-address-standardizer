/*
 * portable_threads.h
 *
 * Thin shim over the lock primitives we use across the extension. Maps
 * directly to pthread on POSIX (incl. MinGW which ships its own pthread)
 * and to SRWLOCK on MSVC. All wrappers are macros so the compiled object
 * code on POSIX is identical to a direct pthread call.
 *
 * Surface used by callers:
 *   port_mutex_t              — opaque mutex type
 *   port_mutex_init(&m)       — initialize (returns 0 on success)
 *   port_mutex_lock(&m)       — exclusive acquire
 *   port_mutex_unlock(&m)     — release
 *   port_mutex_destroy(&m)    — destroy (no-op on MSVC; SRWLOCK has no destructor)
 *   PORT_THREAD_LOCAL         — thread-local storage class
 */

#ifndef PORTABLE_THREADS_H
#define PORTABLE_THREADS_H

#if defined(_MSC_VER)

#include <windows.h>

typedef SRWLOCK port_mutex_t;

#define port_mutex_init(m)    (InitializeSRWLock(m), 0)
#define port_mutex_lock(m)    AcquireSRWLockExclusive(m)
#define port_mutex_unlock(m)  ReleaseSRWLockExclusive(m)
#define port_mutex_destroy(m) ((void)(m), 0)

#define PORT_THREAD_LOCAL __declspec(thread)

#else  /* POSIX (Linux, macOS, MinGW) */

#include <pthread.h>

typedef pthread_mutex_t port_mutex_t;

#define port_mutex_init(m)    pthread_mutex_init((m), NULL)
#define port_mutex_lock(m)    pthread_mutex_lock(m)
#define port_mutex_unlock(m)  pthread_mutex_unlock(m)
#define port_mutex_destroy(m) pthread_mutex_destroy(m)

#define PORT_THREAD_LOCAL _Thread_local

#endif

#endif /* PORTABLE_THREADS_H */
