"""Compatibility shim for event-driven filter.

This module re-exports the refactored event-driven swap filter from
``scanner.event_filter`` so existing imports continue to work.
"""
from scanner.event_filter import CandidatePool, EventDrivenFilter, SWAP_TOPIC

__all__ = ["CandidatePool", "EventDrivenFilter", "SWAP_TOPIC"]
