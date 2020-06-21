from enum import Enum


class STF(str, Enum):
    """
    Short Term Frontier Strategy
    """
    random = "random"
    old_pages_first = "old_pages_first"
    new_pages_first = "new_pages_first"
    change_rate = "change_rate"


class LTF(str, Enum):
    """
    Long Term Frontier Strategy
    """
    random = "random"
    top_level_domain = "top_level_domain"
    large_sites_first = "large_sites_first"
    small_sites_first = "small_sites_first"
    old_sites_first = "old_sites_first"
    page_rank = "page_rank"
    geo_distance = "geo_distance"
    average_change_rate = "average_change_rate"
    consistent_hashing = "consistent_hashing"


class LPPDISTR(str, Enum):
    """
    Links per Page Distribution Type
    """
    discrete = "discrete"
    linear_smaller = "linear_smaller"
    power_law = "power_law"


class ACADEMICS(str, Enum):
    ampere = "Ampere"
    avogadro = "Avogadro"
    bacon = "Bacon"
    bernoulli = "Bernoulli"
    copernicus = "Copernicus"
    curie = "Curie"
    darwin = "Darwin"
    drake = "Drake"
    einstein = "Einstein"
    euler = "Euler"
    fibonacci = "Fibonacci"
    fermat = "Fermat"
    gauss = "Gauss"
    gibbs = "Gibbs"
    hilbert = "Hilbert"
    hopper = "Hopper"
    hawking = "Hawking"
    kepler = "Kepler"
    lovelace = "Lovelace"
    mendel = "Mendel"
    maxwell = "Maxwell"
    newton = "Newton"
    planck = "Planck"
