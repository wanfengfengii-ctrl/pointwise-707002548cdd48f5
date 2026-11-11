"""Single verification entry: full behavior regression plus structure checks.

Runs the unchanged checks.py regression (normal/recovery/edge cases) and the
new structural boundary verification in one process, failing on the first
error. Invoked by .pointwise-runtime.json.
"""

import checks
import verify_structure


def main():
    checks.regression("normal")
    checks.regression("recovery")
    checks.edge_checks()
    print("BEHAVIOR_REGRESSION_OK")
    verify_structure.check_dependency_direction()
    verify_structure.check_in_memory_double()
    verify_structure.check_domain_purity()
    verify_structure.check_disk_recovery()
    print("ALL_VERIFY_OK")


if __name__ == "__main__":
    main()
