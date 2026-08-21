# Shared compile/link options for every cornet target, in one place so the
# three library targets cannot drift apart.
#   - non-Debug: -O2 -funroll-loops -march=native
#   - Debug:     AddressSanitizer + debug info
# Link options are PUBLIC: a static library has no link step of its own, so
# PRIVATE link options would be silently dropped; executables must inherit
# -fsanitize=address in Debug for the asan symbols to resolve.
function(cornet_target_opts target)
    target_compile_options(${target} PRIVATE
        $<$<NOT:$<CONFIG:Debug>>:-O2 -funroll-loops -march=native>
        $<$<CONFIG:Debug>:-fsanitize=address -g>
    )
    target_link_options(${target} PUBLIC
        $<$<CONFIG:Debug>:-fsanitize=address>
    )
endfunction()
