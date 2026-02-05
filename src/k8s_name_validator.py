# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Kubernetes resource name validation for RFC1123 compliance."""

import logging
from typing import Optional

log = logging.getLogger(__name__)

# Define character sets for validation
LOWERCASE_LETTERS = set("abcdefghijklmnopqrstuvwxyz")
DIGITS = set("0123456789")
SEPARATORS = set("-.")
ALPHANUMERIC_LOWER = LOWERCASE_LETTERS | DIGITS
VALID_NAME_CHARS = ALPHANUMERIC_LOWER | SEPARATORS

MAX_NAME_LENGTH = 253


class NameValidationError(Exception):
    """Raised when a Kubernetes resource name violates RFC1123 rules."""

    pass


def validate_resource_name(name_to_check: str, resource_type: str = "Resource") -> None:
    """Verify that a resource name meets Kubernetes RFC1123 subdomain requirements.

    Kubernetes requires resource names to be RFC1123 subdomains which means:
    - Maximum 253 characters
    - Only lowercase alphanumeric, hyphen, or period characters
    - Must begin with an alphanumeric character
    - Must conclude with an alphanumeric character

    Args:
        name_to_check: The resource name to validate
        resource_type: Type of resource for error messaging

    Raises:
        NameValidationError: If the name violates any RFC1123 rule
    """
    if not name_to_check:
        raise NameValidationError(f"{resource_type} name cannot be empty or None")

    name_len = len(name_to_check)

    if name_len > MAX_NAME_LENGTH:
        raise NameValidationError(
            f"{resource_type} name '{name_to_check}' is too long ({name_len} characters). "
            f"Maximum allowed is {MAX_NAME_LENGTH} characters"
        )

    # Validate starting character
    first_char = name_to_check[0]
    if first_char not in ALPHANUMERIC_LOWER:
        raise NameValidationError(
            f"{resource_type} name '{name_to_check}' starts with '{first_char}' which is invalid. "
            f"Names must begin with a lowercase letter (a-z) or digit (0-9)"
        )

    # Validate ending character
    last_char = name_to_check[-1]
    if last_char not in ALPHANUMERIC_LOWER:
        raise NameValidationError(
            f"{resource_type} name '{name_to_check}' ends with '{last_char}' which is invalid. "
            f"Names must end with a lowercase letter (a-z) or digit (0-9)"
        )

    # Validate all characters
    name_chars = set(name_to_check)
    invalid_chars = name_chars - VALID_NAME_CHARS

    if invalid_chars:
        # Create helpful error message based on what's wrong
        error_details = []
        if any(c.isupper() for c in invalid_chars):
            error_details.append("uppercase letters (use lowercase instead)")
        if "_" in invalid_chars:
            error_details.append("underscores (use hyphens instead)")
        if " " in invalid_chars:
            error_details.append("spaces (use hyphens instead)")

        other_invalid = invalid_chars - set("ABCDEFGHIJKLMNOPQRSTUVWXYZ_ ")
        if other_invalid:
            char_list = ", ".join(f"'{c}'" for c in sorted(other_invalid))
            error_details.append(f"invalid characters: {char_list}")

        detail_str = "; ".join(error_details)
        raise NameValidationError(
            f"{resource_type} name '{name_to_check}' contains {detail_str}. "
            f"Only lowercase letters, digits, hyphens (-), and periods (.) are permitted"
        )

    log.debug(f"Validated {resource_type} name: '{name_to_check}'")


def get_validation_error(name_to_check: str, resource_type: str = "Resource") -> Optional[str]:
    """Check if a name is valid and return an error message if not.

    This is a non-throwing version of validate_resource_name that returns
    an error string instead of raising an exception.

    Args:
        name_to_check: The resource name to validate
        resource_type: Type of resource for error messaging

    Returns:
        Error message if invalid, None if valid
    """
    try:
        validate_resource_name(name_to_check, resource_type)
        return None
    except NameValidationError as e:
        return str(e)
