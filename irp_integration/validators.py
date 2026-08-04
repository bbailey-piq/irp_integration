"""
Input validation utilities for IRP Integration module.

Provides reusable validation functions that raise descriptive
IRPValidationError exceptions when validation fails.
"""

import os
from typing import Any, List
from .exceptions import IRPValidationError


def _is_int(value: Any) -> bool:
    """
    Report whether a value is an integer, counting ``bool`` as not one.

    ``bool`` subclasses ``int``, so a plain ``isinstance`` check would accept
    ``True`` where an ID or a page size belongs and send JSON ``true`` to the
    API.

    Args:
        value: Value to test

    Returns:
        True if value is an int and not a bool
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    """
    Report whether a value is an int or float, counting ``bool`` as neither.

    See ``_is_int`` for why ``bool`` is excluded.

    Args:
        value: Value to test

    Returns:
        True if value is an int or float and not a bool
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_non_empty_string(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-empty string.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-empty string
    """
    if not isinstance(value, str):
        raise IRPValidationError(
            f"{param_name} must be a string, got {type(value).__name__}"
        )
    if not value.strip():
        raise IRPValidationError(f"{param_name} cannot be empty")


def validate_positive_int(value: Any, param_name: str) -> None:
    """
    Validate that a value is a positive integer.

    ``bool`` is rejected; see ``_is_int``.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a positive integer
    """
    if not _is_int(value):
        raise IRPValidationError(
            f"{param_name} must be an integer, got {type(value).__name__}"
        )
    if value <= 0:
        raise IRPValidationError(
            f"{param_name} must be positive, got {value}"
        )
    

def validate_non_negative_int(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-negative integer.

    ``bool`` is rejected; see ``_is_int``.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-negative integer
    """
    if not _is_int(value):
        raise IRPValidationError(
            f"{param_name} must be an integer, got {type(value).__name__}"
        )
    if value < 0:
        raise IRPValidationError(
            f"{param_name} must be non-negative, got {value}"
        )


def validate_max_length(value: Any, param_name: str, max_length: int) -> None:
    """
    Validate that a string is no longer than a server-side limit.

    Raises rather than truncating: a shortened value can collide two distinct
    inputs into one.

    Args:
        value: Value to validate
        param_name: Parameter name for error message
        max_length: Maximum permitted number of characters

    Raises:
        IRPValidationError: If value is not a string, or is longer than
            max_length
    """
    if not isinstance(value, str):
        raise IRPValidationError(
            f"{param_name} must be a string, got {type(value).__name__}"
        )
    if len(value) > max_length:
        raise IRPValidationError(
            f"{param_name} must be at most {max_length} characters, "
            f"got {len(value)}: {value!r}"
        )


def validate_file_exists(file_path: str, param_name: str = "file_path") -> None:
    """
    Validate that a file exists at the given path.

    Args:
        file_path: Path to file
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If file does not exist
    """
    if not os.path.exists(file_path):
        raise IRPValidationError(
            f"{param_name} does not exist: {file_path}"
        )
    if not os.path.isfile(file_path):
        raise IRPValidationError(
            f"{param_name} is not a file: {file_path}"
        )


def validate_list_not_empty(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-empty list.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-empty list
    """
    if not isinstance(value, list):
        raise IRPValidationError(
            f"{param_name} must be a list, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise IRPValidationError(f"{param_name} cannot be empty")
    

def validate_list_of_positive_ints(value: Any, param_name: str) -> None:
    """
    Validate that a value is a list containing only positive integers.

    An empty list is accepted; callers that require at least one element
    should enforce that themselves.

    ``bool`` is rejected; see ``_is_int``.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a list, or any element is not a
            positive integer
    """
    if not isinstance(value, list):
        raise IRPValidationError(
            f"{param_name} must be a list, got {type(value).__name__}"
        )
    for index, item in enumerate(value):
        if not _is_int(item):
            raise IRPValidationError(
                f"{param_name}[{index}] must be an integer, got {type(item).__name__}"
            )
        if item <= 0:
            raise IRPValidationError(
                f"{param_name}[{index}] must be positive, got {item}"
            )


def validate_positive_float(value: Any, param_name: str) -> None:
    """
    Validate that a value is a positive float.

    An int is accepted; ``bool`` is not.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a positive float
    """
    if not _is_number(value):
        raise IRPValidationError(
            f"{param_name} must be a float, got {type(value).__name__}"
        )
    if value <= 0:
        raise IRPValidationError(
            f"{param_name} must be positive, got {value}"
        )
    
def validate_non_negative_float(value: Any, param_name: str) -> None:
    """
    Validate that a value is a non-negative float.

    An int is accepted; ``bool`` is not.

    Args:
        value: Value to validate
        param_name: Parameter name for error message

    Raises:
        IRPValidationError: If value is not a non-negative float
    """
    if not _is_number(value):
        raise IRPValidationError(
            f"{param_name} must be a float, got {type(value).__name__}"
        )
    if value < 0:
        raise IRPValidationError(
            f"{param_name} must be non-negative, got {value}"
        )