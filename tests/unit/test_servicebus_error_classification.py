"""R2.1d item 2 / R2.1e item 6: every ServiceBusError subclass in the
installed SDK must have an explicit, documented classification — no silent
fallthrough.

This recursively enumerates ``ServiceBusError.__subclasses__()`` from the
*installed* azure-servicebus SDK (not a hardcoded list) so a future SDK
upgrade that adds a new subclass fails this test loudly instead of quietly
inheriting the base-class default with nobody having decided whether that's
correct for that specific class.

``__subclasses__()`` only sees classes whose *module has actually been
imported* — a subclass defined in a submodule this test never touches
(directly or transitively) would be invisible to it and silently excluded
from coverage, which is exactly the kind of gap this test exists to catch.
``azure.servicebus.exceptions`` (imported below) pulls in the ones current
azure-servicebus versions define, but nothing guarantees a future version
keeps defining all of them there — so before walking __subclasses__(), this
imports every ``azure.servicebus`` submodule via ``pkgutil.walk_packages``,
not just the one the classifier itself imports from.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil

import azure.servicebus as sb
import azure.servicebus.exceptions as sbe
import pytest

from function_app import _DETERMINISTIC_SERVICE_BUS_ERRORS, _is_transient_single

_logger = logging.getLogger(__name__)


def _import_every_servicebus_submodule() -> None:
    """Best-effort import of every azure.servicebus submodule, so any
    ServiceBusError subclass defined anywhere in the package (not just in
    .exceptions) is loaded and therefore visible to __subclasses__().

    Best-effort, not required-to-succeed: some submodules (e.g. optional
    transport backends, or ones with an extra dependency not installed)
    may be unimportable in a given environment for reasons unrelated to
    exception classification. Catches ``Exception`` broadly (not just
    ``ImportError``) — a module's top-level code can fail in other ways
    (e.g. a missing optional C extension raising something else entirely)
    — and logs rather than silently swallowing, so an unexpected failure
    is still visible without failing this test over an unrelated import
    problem in a module irrelevant to exception classification.
    """
    for module_info in pkgutil.walk_packages(sb.__path__, prefix=f"{sb.__name__}."):
        try:
            importlib.import_module(module_info.name)
        except Exception:
            _logger.warning(
                "Could not import %s while enumerating ServiceBusError "
                "subclasses — continuing (best-effort)",
                module_info.name,
                exc_info=True,
            )
            continue


_import_every_servicebus_submodule()

# Expected classification for every ServiceBusError subclass the installed
# SDK ships, keyed by class name. True = transient (redeliver), False =
# deterministic (dead-letter, never redeliver).
#
# The curated deterministic set the reviewer named explicitly: entity
# gone/disabled, auth failures, a permanently-too-big message. Also added
# by judgment (retrying cannot help either): MessagingEntityAlreadyExistsError
# (creating an entity that already exists never stops already existing) and
# MessageNotFoundError (a message that doesn't exist at a given sequence
# number won't exist on retry either).
_EXPECTED_DETERMINISTIC = {
    "MessagingEntityNotFoundError",
    "MessagingEntityDisabledError",
    "MessagingEntityAlreadyExistsError",
    "ServiceBusAuthenticationError",
    "ServiceBusAuthorizationError",
    "MessageSizeExceededError",
    "MessageNotFoundError",
}

# Explicitly transient per the reviewer's instruction: a retry, possibly
# after backoff, can succeed once quota frees up or a new lock is acquired.
# Plus the pre-existing transport/throttle/timeout classes, and the
# lock-renewal/session-lock classes (a fresh receive attempt gets a fresh
# lock) — none of these are permanent, so all fall through to the
# base-class transient default.
_EXPECTED_TRANSIENT = {
    "ServiceBusConnectionError",
    "ServiceBusCommunicationError",
    "ServiceBusServerBusyError",
    "OperationTimeoutError",
    "ServiceBusQuotaExceededError",
    "MessageLockLostError",
    "SessionLockLostError",
    "AutoLockRenewFailed",
    "AutoLockRenewTimeout",
    "SessionCannotBeLockedError",
}


def _all_subclasses(cls: type) -> set[type]:
    subclasses: set[type] = set()
    for sub in cls.__subclasses__():
        subclasses.add(sub)
        subclasses |= _all_subclasses(sub)
    return subclasses


def _instantiate(cls: type) -> BaseException:
    try:
        return cls(message="synthetic test exception")
    except TypeError:
        return cls()


_INSTALLED_SUBCLASSES = sorted(
    _all_subclasses(sbe.ServiceBusError), key=lambda c: c.__name__
)


class TestEveryInstalledSubclassHasAnExplicitClassification:
    def test_no_installed_subclass_is_unclassified(self) -> None:
        """Fails loudly (naming the class) if the installed SDK has a
        ServiceBusError subclass this test's expectation tables don't
        mention — an unclassified class must not silently pass."""
        known = _EXPECTED_DETERMINISTIC | _EXPECTED_TRANSIENT
        installed_names = {cls.__name__ for cls in _INSTALLED_SUBCLASSES}
        unclassified = installed_names - known
        assert not unclassified, (
            f"ServiceBusError subclass(es) with no explicit expected "
            f"classification in this test: {sorted(unclassified)} — add "
            f"them to _EXPECTED_DETERMINISTIC or _EXPECTED_TRANSIENT."
        )

    def test_no_expectation_table_entry_is_stale(self) -> None:
        """The inverse: an expectation naming a class the installed SDK no
        longer ships would silently stop testing anything."""
        known = _EXPECTED_DETERMINISTIC | _EXPECTED_TRANSIENT
        installed_names = {cls.__name__ for cls in _INSTALLED_SUBCLASSES}
        stale = known - installed_names
        assert not stale, (
            f"Expectation table names classes not in the installed SDK: {sorted(stale)}"
        )

    @pytest.mark.parametrize("cls", _INSTALLED_SUBCLASSES, ids=lambda c: c.__name__)
    def test_classification_matches_expectation(self, cls: type) -> None:
        name = cls.__name__
        assert name in _EXPECTED_DETERMINISTIC or name in _EXPECTED_TRANSIENT, (
            f"{name} has no expected classification — see "
            f"test_no_installed_subclass_is_unclassified"
        )
        expected_transient = name in _EXPECTED_TRANSIENT
        exc = _instantiate(cls)
        assert _is_transient_single(exc) is expected_transient

    def test_deterministic_set_matches_function_app_constant(self) -> None:
        """The classifier's actual deterministic tuple must match this
        test's expectation set exactly — proves the two never drift apart."""
        actual_names = {cls.__name__ for cls in _DETERMINISTIC_SERVICE_BUS_ERRORS}
        assert actual_names == _EXPECTED_DETERMINISTIC

    def test_service_bus_error_base_itself_is_transient(self) -> None:
        """The base class, raised directly (not via a named subclass), must
        default to transient — the safe default for Service Bus."""
        assert _is_transient_single(sbe.ServiceBusError(message="generic")) is True
