"""Provide helper functions for RPC."""

__all__ = ['rpc_export_function', 'rpc_export_generator']


def rpc_export_function(func):
  """Export a function for RPC."""
  setattr(func, 'rpc_export', 'function')
  return func


def rpc_export_generator(func):
  """Export a generator for RPC."""
  setattr(func, 'rpc_export', 'generator')
  return func
