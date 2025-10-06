from typing import Tuple, Optional
import numpy as np
from scipy.stats import fisher_exact
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def calculate_fisher_exact(a: int, b: int, c: int, d: int) -> Tuple[float, float]:
    contingency_table = [[a, b], [c, d]]
    oddsratio, pvalue = fisher_exact(contingency_table, alternative='two-sided')
    return oddsratio, pvalue


def calculate_risk_difference(a: int, b: int, c: int, d: int, 
                                continuity_correction: float = 0.0) -> Tuple[float, float, float]:
    n1 = a + b
    n2 = c + d
    
    if continuity_correction > 0:
        a_adj = a + continuity_correction
        b_adj = b + continuity_correction
        c_adj = c + continuity_correction
        d_adj = d + continuity_correction
        n1_adj = n1 + 2 * continuity_correction
        n2_adj = n2 + 2 * continuity_correction
    else:
        a_adj, b_adj, c_adj, d_adj = a, b, c, d
        n1_adj, n2_adj = n1, n2
    
    p1 = a_adj / n1_adj if n1_adj > 0 else 0
    p2 = c_adj / n2_adj if n2_adj > 0 else 0
    
    risk_diff = p1 - p2
    
    se = np.sqrt((p1 * (1 - p1) / n1_adj) + (p2 * (1 - p2) / n2_adj)) if n1_adj > 0 and n2_adj > 0 else 0
    
    ci_lower = risk_diff - 1.96 * se
    ci_upper = risk_diff + 1.96 * se
    
    return risk_diff, ci_lower, ci_upper


def calculate_relative_risk(a: int, b: int, c: int, d: int,
                              continuity_correction: float = 0.0) -> Tuple[float, float, float]:
    n1 = a + b
    n2 = c + d
    
    if continuity_correction > 0:
        a_adj = a + continuity_correction
        c_adj = c + continuity_correction
        n1_adj = n1 + 2 * continuity_correction
        n2_adj = n2 + 2 * continuity_correction
    else:
        a_adj, c_adj = a, c
        n1_adj, n2_adj = n1, n2
    
    p1 = a_adj / n1_adj if n1_adj > 0 else 0
    p2 = c_adj / n2_adj if n2_adj > 0 else 0
    
    rr = p1 / p2 if p2 > 0 else float('inf')
    
    if a_adj > 0 and c_adj > 0:
        log_rr = np.log(rr)
        se_log_rr = np.sqrt((1/a_adj - 1/n1_adj) + (1/c_adj - 1/n2_adj))
        ci_lower = np.exp(log_rr - 1.96 * se_log_rr)
        ci_upper = np.exp(log_rr + 1.96 * se_log_rr)
    else:
        ci_lower = 0
        ci_upper = float('inf')
    
    return rr, ci_lower, ci_upper


def calculate_odds_ratio(a: int, b: int, c: int, d: int,
                          continuity_correction: float = 0.0) -> Tuple[float, float, float]:
    if continuity_correction > 0:
        a_adj = a + continuity_correction
        b_adj = b + continuity_correction
        c_adj = c + continuity_correction
        d_adj = d + continuity_correction
    else:
        a_adj, b_adj, c_adj, d_adj = a, b, c, d
    
    or_value = (a_adj * d_adj) / (b_adj * c_adj) if b_adj > 0 and c_adj > 0 else float('inf')
    
    if a_adj > 0 and b_adj > 0 and c_adj > 0 and d_adj > 0:
        log_or = np.log(or_value)
        se_log_or = np.sqrt(1/a_adj + 1/b_adj + 1/c_adj + 1/d_adj)
        ci_lower = np.exp(log_or - 1.96 * se_log_or)
        ci_upper = np.exp(log_or + 1.96 * se_log_or)
    else:
        ci_lower = 0
        ci_upper = float('inf')
    
    return or_value, ci_lower, ci_upper


def apply_continuity_correction(a: int, b: int, c: int, d: int,
                                  cc_method: str = "0.5") -> Tuple[float, float, float, float]:
    if cc_method == "0":
        return float(a), float(b), float(c), float(d)
    elif cc_method == "0.5":
        return a + 0.5, b + 0.5, c + 0.5, d + 0.5
    elif cc_method == "1":
        return a + 1, b + 1, c + 1, d + 1
    elif cc_method == "arm":
        n1 = a + b
        n2 = c + d
        cc = 1 / (n1 + n2) if (n1 + n2) > 0 else 0
        return a + cc, b + cc, c + cc, d + cc
    else:
        return float(a), float(b), float(c), float(d)
