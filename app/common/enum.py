from enum import Enum


class LONGPRIO(str, Enum):
    """
    Long Term Frontier Strategy
    """
    random = "random"
    large_sites_first = "large_sites_first"
    small_sites_first = "small_sites_first"
    old_sites_first = "old_sites_first"
    new_sites_first = "new_sites_first"
    avg_pagerank = "avg_pagerank"
    avg_change_rate = "avg_change_rate"


class LONGPART(str, Enum):
    """
    Strategy for Partitioning the Long Term Frontier
    """
    none = "none"
    top_level_domain = "top_level_domain"
    fqdn_hash = "fqdn_hash"
    consistent_hashing = "consistent_hashing"


class SHORTPRIO(str, Enum):
    """
    Short Term Frontier Strategy
    """
    random = "random"
    old_pages_first = "old_pages_first"
    new_pages_first = "new_pages_first"
    pagerank = "pagerank"
    change_rate = "change_rate"


class PAGELINKDISTR(str, Enum):
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
