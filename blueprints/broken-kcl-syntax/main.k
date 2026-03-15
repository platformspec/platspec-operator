# Intentionally broken blueprint — do not fix.
# Used to test operator error handling for KCL render failures.
# The operator should record Rendered=False and set platform phase=Progressing
# without crashing or blocking other bindings from completing.

this_is_not_valid_kcl = undefined_variable +++ ???
