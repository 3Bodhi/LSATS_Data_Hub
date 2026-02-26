"""
Compliance management scripts.

Scripts for automating computer compliance notifications,
ticket creation, and follow-up processes.
"""

# Optionally expose main functions
try:
    from .compliance_ticket_automator import main as automator_main
    from .compliance_ticket_second_outreach import main as second_outreach_main
    from .compliance_ticket_third_outreach import main as third_outreach_main
except ImportError:
    pass

__all__ = ['automator_main', 'second_outreach_main', 'third_outreach_main']
