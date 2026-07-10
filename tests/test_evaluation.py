"""Unit tests for boundary evaluation. Run: python -m pytest tests/ (or python tests/test_evaluation.py)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.evaluation import evaluate


def test_exact_match():
    e = evaluate([1, 5, 9], [1, 5, 9])
    assert e.exact_match
    assert e.precision == e.recall == e.f1 == 1.0
    assert e.multidoc_correct


def test_off_by_one():
    e = evaluate([1, 5, 9], [1, 6, 9])
    assert not e.exact_match
    assert e.n_offby1 == 1
    assert e.f1 == 0.5          # 6 != 5 exactly
    assert e.f1_tol == 1.0      # within tolerance


def test_false_positive():
    e = evaluate([1, 5], [1, 5, 7])
    assert e.n_false_positive == 1
    assert e.precision == 0.5
    assert e.recall == 1.0


def test_false_negative_multidoc():
    e = evaluate([1, 4, 8], [1, 8])
    assert e.n_false_negative == 1
    assert e.recall == 0.5
    assert e.multidoc_correct          # both still multi-doc


def test_single_document():
    e = evaluate([1], [1])
    assert e.exact_match
    assert not e.gt_is_multidoc
    assert e.multidoc_correct


def test_multidoc_misclassified():
    # GT is multi-doc, model said single
    e = evaluate([1, 6], [1])
    assert not e.multidoc_correct
    assert e.n_false_negative == 1


def test_normalizes_unsorted_and_dupes():
    e = evaluate([9, 1, 5, 5], [1, 9, 5])
    assert e.exact_match


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
