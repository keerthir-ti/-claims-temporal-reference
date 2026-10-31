"""Simulator for the application around the workflows (mock data only).

It stands in for what the client already has: ClaimsPro, eClaims and ImageRight changing claims, the
event bridge delivering their change events to Temporal, and adjusters deciding tasks through the
service behind Kong. Nothing here is part of the Temporal design; it exists to run it end to end.
"""
