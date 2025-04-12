from collections import Counter
import math

def zipf_detect(keys):
    """
    Detects Zipf's law for the given list of keys by calculating the Zipf coefficient 
    and sorting the frequency of keys.
    
    Args:
        keys (list): List of keys (or words) to analyze for Zipf's law.
    
    Returns:
        tuple: Zipf coefficient and sorted frequencies of keys.
    """
    # Count the frequencies of each key
    counter = Counter(keys)
    
    # Sort keys by frequency in descending order
    sorted_freq = counter.most_common()
    
    # Calculate the Zipf coefficient (approximate method)
    n = len(sorted_freq)
    rank = list(range(1, n + 1))
    frequency = [item[1] for item in sorted_freq]
    
    # Calculate the Zipf coefficient using the slope of log(frequency) vs log(rank)
    zipf_coefficient = calculate_zipf_coefficient(rank, frequency)
    
    return zipf_coefficient, sorted_freq

def calculate_zipf_coefficient(rank, frequency):
    """
    Calculates the Zipf coefficient based on the rank and frequency.
    
    Args:
        rank (list): A list of ranks corresponding to frequencies.
        frequency (list): A list of frequencies for each rank.
    
    Returns:
        float: The calculated Zipf coefficient.
    """
    log_rank = [math.log(r) for r in rank]
    log_frequency = [math.log(f) for f in frequency]
    
    # Perform linear regression to estimate the Zipf coefficient
    numerator = sum(r * f for r, f in zip(log_rank, log_frequency))
    denominator = sum(r**2 for r in log_rank)
    
    # Return the Zipf coefficient
    return numerator / denominator if denominator != 0 else 0
