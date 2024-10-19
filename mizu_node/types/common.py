from enum import Enum


class VerificationMode(str, Enum):
    none = "none"
    always = "always"
    random = "random"


class JobType(str, Enum):
    pow = "pow"
    classification = "classification"
