import logging
import sys
import threading
import time
from base64 import b64encode
from logging import getLogger
from os import urandom
from queue import SimpleQueue, Empty

from redis import StrictRedis

__version__ = '3.7.0.1'

CHECK_RENEW_LOCK_THREAD_EVERY = 1000

loggers = {
    k: getLogger(".".join(("redis-lock", k)))
    for k in [
        "acquire",
        "refresh.thread",
        "release",
        "extend"
    ]
}

cornix_handlers = getLogger('cornix').handlers
for _logger in loggers.values():
    _logger.handlers = cornix_handlers
    _logger.setLevel(logging.INFO)

PY3 = sys.version_info[0] == 3

if PY3:
    text_type = str
    binary_type = bytes
else:
    text_type = unicode  # noqa
    binary_type = str


# Check if the id match. If not, return an error code.
UNLOCK_SCRIPT = b"""
    if redis.call("get", KEYS[1]) ~= ARGV[1] then
        return 1
    else
        redis.call("del", KEYS[1])
        return 0
    end
"""

# Covers both cases when key doesn't exist and doesn't equal to lock's id
EXTEND_SCRIPT = b"""
    if redis.call("get", KEYS[1]) ~= ARGV[1] then
        return 1
    elseif redis.call("ttl", KEYS[1]) < 0 then
        return 2
    else
        redis.call("expire", KEYS[1], ARGV[2])
        return 0
    end
"""


class AlreadyAcquired(RuntimeError):
    pass


class NotAcquired(RuntimeError):
    pass


class AlreadyStarted(RuntimeError):
    pass


class TimeoutNotUsable(RuntimeError):
    pass


class InvalidTimeout(RuntimeError):
    pass


class TimeoutTooLarge(RuntimeError):
    pass


class NotExpirable(RuntimeError):
    pass


lock_to_renewal_time = dict()
add_lock_extend_queue = SimpleQueue()
create_thread_lock = threading.Lock()
was_scripts_registered = False


def safe_extend_renewal_time(lock):
    try:
        lock_to_renewal_time[lock] = time.time() + lock.lock_renewal_interval
        return True
    except TypeError:  # this happens when the lock lock_renewal_interval became None
        return False


def extend_locks(logger):
    to_remove_locks = []
    now = time.time()  # outside for - less accurate - better performance.
    for lock, extend_time in lock_to_renewal_time.items():
        if not lock.lock_renewal_interval:
            to_remove_locks.append(lock)
            continue
        if extend_time <= now:
            try:
                if lock.lock_renewal_interval:
                    lock.extend()
                    success = safe_extend_renewal_time(lock)
                    if not success:
                        to_remove_locks.append(lock)
                else:
                    to_remove_locks.append(lock)
            except NotAcquired as e:
                if lock.lock_renewal_interval:
                    logger.exception("Got NotAcquired on extend while renewal is still active %s", e)
                to_remove_locks.append(lock)
            except Exception as e:
                logger.exception("Got exception on extend %s", e)

    return to_remove_locks


def handle_locks_extending():
    logger = loggers["refresh.thread"]
    while True:
        try:
            to_remove_locks = extend_locks(logger)

            for lock in to_remove_locks:
                del lock_to_renewal_time[lock]

            try:
                size = add_lock_extend_queue.qsize()
                for _ in range(size):
                    lock = add_lock_extend_queue.get_nowait()
                    if lock.lock_renewal_interval:
                        safe_extend_renewal_time(lock)
            except Empty:
                pass

            time.sleep(1)
        except Exception as e:
            logger.exception("Got exception on handle_locks_extending %s", e)


class Lock(object):
    """
    A Lock context manager implemented via redis SETNX and one extending thread.
    """
    unlock_script = None
    extend_script = None
    renew_lock_thread = None
    counter = 0

    def __init__(self, redis_class, name, expire=None, id=None, auto_renewal=False, strict=True):
        """
        :param redis_class:
            A class that has conn: an instance of :class:`~StrictRedis`.
        :param name:
            The name (redis key) the lock should have.
        :param expire:
            The lock expiry time in seconds. If left at the default (None)
            the lock will not expire.
        :param id:
            The ID (redis value) the lock should have. A random value is
            generated when left at the default.

            Note that if you specify this then the lock is marked as "held". Acquires
            won't be possible.
        :param auto_renewal:
            If set to ``True``, Lock will automatically renew the lock so that it
            doesn't expire for as long as the lock is held (acquire() called
            or running in a context manager).

            Implementation note: Renewal will happen using a daemon thread with
            an interval of ``expire*2/3``. If wishing to use a different renewal
            time, subclass Lock, call ``super().__init__()`` then set
            ``self._lock_renewal_interval`` to your desired interval.
        :param strict:
            If set ``True`` then the ``redis_client`` needs to be an instance of ``redis.StrictRedis``.
        """
        if strict and not isinstance(redis_class.conn, StrictRedis):
            raise ValueError("redis_client must be instance of StrictRedis. "
                             "Use strict=False if you know what you're doing.")
        if auto_renewal and expire is None:
            raise ValueError("Expire may not be None when auto_renewal is set")

        self.redis_class = redis_class

        if expire:
            expire = int(expire)
            if expire < 0:
                raise ValueError("A negative expire is not acceptable.")
        else:
            expire = None
        self._expire = expire

        if id is None:
            self._id = b64encode(urandom(18)).decode('ascii')  # TODO: improve
        elif isinstance(id, binary_type):
            try:
                self._id = id.decode('ascii')
            except UnicodeDecodeError:
                self._id = b64encode(id).decode('ascii')
        elif isinstance(id, text_type):
            self._id = id
        else:
            raise TypeError("Incorrect type for `id`. Must be bytes/str not %s." % type(id))
        self._name = name
        self.lock_renewal_interval = self.get_renewal_interval(auto_renewal)

        self.register_scripts(self.redis_class.conn)
        self.is_locked = False

        self.start_locking_thread_if_needed()

        self.lock_start_time = None

    @classmethod
    def start_locking_thread_if_needed(cls):
        _counter = cls.counter
        cls.counter += 1

        if _counter % CHECK_RENEW_LOCK_THREAD_EVERY == 0:
            cls.start_renew_lock_thread()

    @classmethod
    def start_renew_lock_thread(cls):
        if cls.renew_lock_thread is None or not cls.renew_lock_thread.is_alive():
            logger = loggers["refresh.thread"]
            with create_thread_lock:
                if cls.renew_lock_thread is None or not cls.renew_lock_thread.is_alive():
                    if cls.renew_lock_thread is None:
                        logger.info("Starting new thread to handle locks extending")
                    else:
                        logger.error("Starting new thread to handle locks extending, the previous one died!")
                    cls.renew_lock_thread = threading.Thread(target=handle_locks_extending)
                    cls.renew_lock_thread.daemon = True
                    cls.renew_lock_thread.start()

    def get_renewal_interval(self, auto_renewal):
        if not auto_renewal:
            return
        if self._expire < 10:
            raise Exception("Expiration is too low to ensure renewal")
        return self._expire * 0.5

    @classmethod
    def register_scripts(cls, redis_client):  # func is called from decorators
        global was_scripts_registered
        if not was_scripts_registered:
            cls.unlock_script = redis_client.register_script(UNLOCK_SCRIPT)
            cls.extend_script = redis_client.register_script(EXTEND_SCRIPT)
            was_scripts_registered = True

    def reset(self):
        """
        Forcibly deletes the lock. Use this with care.
        """
        self.redis_class.conn.delete(self._name)

    @property
    def id(self):
        return self._id

    def acquire(self):
        """
        :param blocking:
            Boolean value specifying whether lock should be blocking or not.
        :param timeout:
            An integer value specifying the maximum number of seconds to block.
        """
        logger = loggers["acquire"]

        logger.debug("Getting lock for %r ...", self._name)

        if self.is_locked:
            raise AlreadyAcquired("Already acquired from this Lock instance.")

        is_locked = not self.redis_class.conn.set(self._name, self._id, nx=True, ex=self._expire)
        if is_locked:
            logger.debug("Failed to get lock on %r.", self._name)
            return False

        self.is_locked = True
        logger.debug("Got lock for %r.", self._name)
        if self.lock_renewal_interval is not None:
            add_lock_extend_queue.put_nowait(self)
        self.lock_start_time = time.time()
        return True

    def extend(self, expire=None):
        """Extends expiration time of the lock.

        :param expire:
            New expiration time. If ``None`` - `expire` provided during
            lock initialization will be taken.
        """
        if expire:
            expire = int(expire)
            if expire < 0:
                raise ValueError("A negative expire is not acceptable.")
        elif self._expire is not None:
            expire = self._expire
        else:
            raise TypeError(
                "To extend a lock 'expire' must be provided as an "
                "argument to extend() method or at initialization time."
            )

        error = self.extend_script(client=self.redis_class.conn, keys=(self._name,), args=(self._id, expire))
        logger = loggers["extend"]
        logger.info("Extending lock for %s", self._name)
        if error == 1:
            raise NotAcquired("Lock %s is not acquired or it already expired." % self._name)
        elif error == 2:
            raise NotExpirable("Lock %s has no assigned expiration time" % self._name)
        elif error:
            raise RuntimeError("Unsupported error code %s from EXTEND script" % error)

    def release(self):
        """Releases the lock, that was acquired with the same object.

        .. note::

            If you want to release a lock that you acquired in a different place you have two choices:

            * Use ``Lock("name", id=id_from_other_place).release()``
            * Use ``Lock("name").reset()``
        """
        if not self.is_locked:
            return
        if self.lock_renewal_interval is not None:
            self.lock_renewal_interval = None  # "signals the no extend required"

        loggers["release"].debug("Releasing %r.", self._name)
        error = self.unlock_script(client=self.redis_class.conn, keys=(self._name,), args=(self._id,))
        if error == 1:
            raise NotAcquired("Lock %s is not acquired or it already expired." % self._name)
        elif error:
            raise RuntimeError("Unsupported error code %s from EXTEND script." % error)
        self.is_locked = False

    def locked(self):
        """
        Return true if the lock is acquired.

        Checks that lock with same name already exists. This method returns true, even if
        lock have another id.
        """
        return self.redis_class.conn.exists(self._name) == 1


def multi_lock(redis_client, lock_name_list: list[str], ttl: int, auto_renewal=False) -> list[Lock]:
    lock_start_time = time.time()
    lock_obj_list: list[Lock] = []
    for lock_name in lock_name_list:
        lock_obj = Lock(redis_client, name=lock_name, expire=ttl, auto_renewal=auto_renewal, strict=False)
        lock_obj_list.append(lock_obj)
    with redis_client.pipeline() as pipe:
        for lock_obj in lock_obj_list:
            pipe.set(lock_obj._name, lock_obj._id, nx=True, ex=lock_obj._expire)
        results = pipe.execute()
    for i, result in enumerate(results):
        if result:
            lock_obj_list[i].is_locked = True
            lock_obj_list[i].lock_start_time = lock_start_time
            if lock_obj_list[i].lock_renewal_interval is not None:
                add_lock_extend_queue.put_nowait(lock_obj_list[i])
    return lock_obj_list


def multi_unlock(redis_client, lock_objs: list[Lock]):
    ttl = lock_objs[0]._expire or 0
    lock_start_time = lock_objs[0].lock_start_time
    if (time.time() - lock_start_time) < (ttl // 2):
        with redis_client.pipeline() as pipe:
            for lock_obj in lock_objs:
                pipe.get(lock_obj._name)
            results = pipe.execute()
        for i, result in enumerate(results):
            if result != lock_objs[i]._id:
                break
        else:
            with redis_client.pipeline() as pipe:
                for lock_obj in lock_objs:
                    if lock_obj.lock_renewal_interval is not None:
                        lock_obj.lock_renewal_interval = None
                    pipe.delete(lock_obj._name)
                pipe.execute()
                return
    for lock_obj in lock_objs:
        try:
            lock_obj.release()
        except NotAcquired:
            pass
