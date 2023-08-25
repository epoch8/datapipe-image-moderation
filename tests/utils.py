import pandas as pd

from datapipe.datatable import DataTable
from datapipe.types import DataDF


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert a == b


def assert_df_equal(a: pd.DataFrame, b: pd.DataFrame, index_cols=["id"], check_only_idx=False) -> bool:
    a = a.set_index(index_cols)
    b = b.set_index(index_cols)

    assert_idx_equal(a.index, b.index)

    if check_only_idx:
        return True

    eq_rows = (a.sort_index() == b.sort_index()).all(axis="columns")

    if eq_rows.all():
        return True

    else:
        print("Difference")
        print("A:")
        print(a.loc[-eq_rows])
        print("B:")
        print(b.loc[-eq_rows])

        raise AssertionError


def assert_datatable_equal(a: DataTable, b: DataDF, check_only_idx=False) -> bool:
    return assert_df_equal(a.get_data(), b, index_cols=a.primary_keys, check_only_idx=check_only_idx)
