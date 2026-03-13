

// --- GLOBAL NETWORK INTERCEPTOR ---
// Ensures all requests (fetch/xhr) include SID and CSRF when necessary
(function () {
    const originalFetch = window.fetch;
    window.fetch = async (...args) => {
        let [resource, config] = args;

        // 1. SID Persistence Logic
        const sid = window.SID || new URLSearchParams(window.location.search).get('sid');
        if (sid) {
            try {
                let url = new URL(resource, window.location.origin);
                // Only for internal requests
                if (url.origin === window.location.origin && !url.searchParams.has('sid')) {
                    url.searchParams.set('sid', sid);
                    resource = url.toString();
                }
            } catch (e) { /* ignore safe errors */ }
        }

        // 2. CSRF Injection for POST/PUT/DELETE
        if (config && config.method && ['POST', 'PUT', 'DELETE'].includes(config.method.toUpperCase())) {
            config.headers = config.headers || {};
            const csrfToken = document.querySelector('input[name="csrf_token"]')?.value ||
                document.querySelector('meta[name="csrf-token"]')?.content;

            if (csrfToken && !config.headers['X-CSRF-Token']) {
                config.headers['X-CSRF-Token'] = csrfToken;
            }
        }

        try {
            const response = await originalFetch(resource, config);

            // Global Error Handling
            if (!response.ok) {
                if (response.status === 401) {
                    showToast("Sesión expirada. Por favor, vuelva a ingresar.", "error");
                    setTimeout(() => window.location.href = '/login', 2000);
                } else if (response.status >= 500) {
                    let errorDetails = "Ocurrió una falla interna en el servidor. El equipo técnico ha sido notificado.";
                    try {
                        const contentType = response.headers.get("content-type");
                        if (contentType && contentType.includes("application/json")) {
                            const data = await response.clone().json();
                            errorDetails = data.error || data.message || JSON.stringify(data);
                        } else {
                            const errorText = await response.clone().text();
                            if (errorText && errorText.length < 500 && !errorText.toLowerCase().includes('<!doctype')) {
                                errorDetails = errorText;
                            }
                        }
                    } catch (e) { }

                    // Fallback visual via console so at least developers can trace it
                    console.error("Intercepted 5xx response for:", resource, "Details:", errorDetails);

                    if (typeof Swal !== 'undefined') {
                        // Avoid duplicates if a server error is already on screen
                        if (document.querySelector('.premium-system-error')) return response;

                        Swal.fire({
                            icon: 'error',
                            title: '<span style="color:#ef4444; font-weight: 800; font-size: 1.1rem; text-transform: uppercase;"><i class="fas fa-server"></i> ERROR DEL SERVIDOR (' + response.status + ')</span>',
                            html: `
                                <div style="text-align: left; margin-top: 15px;">
                                    <div style="background: rgba(239, 68, 68, 0.1); border-left: 4px solid #ef4444; padding: 15px; border-radius: 5px; color: #fca5a5; font-size: 0.95rem; line-height: 1.5; font-family: monospace; overflow-x: auto;">
                                        ${errorDetails.replace(/</g, "&lt;").replace(/>/g, "&gt;")}
                                    </div>
                                    <div style="margin-top: 15px; font-size: 0.8rem; color: #94a3b8;">
                                        <i>La petición ha fallado por un problema interno. Se requiere intervención o corrección técnica de los datos.</i>
                                    </div>
                                </div>
                            `,
                            background: '#111827',
                            color: '#fff',
                            confirmButtonColor: '#ef4444',
                            confirmButtonText: 'ENTENDIDO',
                            width: '600px',
                            allowOutsideClick: false, // This ensures it won't disappear and forces the user to close it manually!
                            customClass: { popup: 'premium-system-error' }
                        });
                    } else {
                        // Fallback purely relying on toast if swal isn't loaded
                        showToast("Error Interno del Servidor", "error");
                    }
                }
            }
            return response;
        } catch (error) {
            showToast("Error de conexión: No se pudo contactar con el servidor.", "error");
            throw error;
        }
    };

    // jQuery AJAX Interceptor (if jQuery is present)
    let ajaxRetries = 0;
    const setupAjaxInterceptor = () => {
        if (window.jQuery) {
            $(document).ajaxSend(function (event, jqXHR, settings) {
                const sid = window.SID || new URLSearchParams(window.location.search).get('sid');
                if (sid && settings.url.indexOf('sid=') === -1) {
                    settings.url += (settings.url.indexOf('?') === -1 ? '?' : '&') + 'sid=' + sid;
                }

                if (settings.type.toUpperCase() !== 'GET') {
                    const csrfToken = document.querySelector('input[name="csrf_token"]')?.value ||
                        document.querySelector('meta[name="csrf-token"]')?.content;
                    if (csrfToken) {
                        jqXHR.setRequestHeader('X-CSRF-Token', csrfToken);
                    }
                }
            });
        } else if (ajaxRetries < 10) {
            ajaxRetries++;
            // Try again in 500ms if jQuery is expected but not yet loaded
            setTimeout(setupAjaxInterceptor, 500);
        }
    };
    setupAjaxInterceptor();
})();

function toggleModal(modalId) {
    const m = document.getElementById(modalId);
    let backdrop = document.getElementById('modal-backdrop');

    // Ensure backdrop exists and has basic functionality
    if (!backdrop) {
        backdrop = document.createElement('div');
        backdrop.id = 'modal-backdrop';
        backdrop.className = 'modal-backdrop';
        document.body.appendChild(backdrop);
    }

    // Always ensure styles are applied if they come from JS (fallback for missing class)
    if (!backdrop.classList.contains('modal-backdrop')) {
        backdrop.className = 'modal-backdrop';
    }

    // Global click-to-close handler for the backdrop (only if not already set)
    if (!backdrop.dataset.hasHandler) {
        backdrop.onclick = () => {
            if (document.body.classList.contains('force-modal')) return;
            // Close all visible modals
            const openModals = document.querySelectorAll('.modal-animate, .card[style*="display:block"], .modal-premium[style*="display: block"]');
            openModals.forEach(mod => {
                const id = mod.id;
                if (id) toggleModal(id);
            });
        };
        backdrop.dataset.hasHandler = "true";
    }

    if (m) {
        const isOpening = (m.style.display === 'none' || m.style.display === '');

        if (isOpening) {
            m.style.display = 'block';
            backdrop.style.display = 'block';
            // Use requestAnimationFrame for smoother transition
            requestAnimationFrame(() => {
                backdrop.style.opacity = '1';
                backdrop.style.pointerEvents = 'auto';
            });
            m.classList.add('modal-animate');
            document.body.style.overflow = 'hidden';
        } else {
            m.style.display = 'none';

            // Check if there are other modals still open before hiding backdrop
            const otherOpen = Array.from(document.querySelectorAll('.modal-animate, .card[style*="display:block"], .modal-premium[style*="display: block"]'))
                .filter(mod => mod.id !== modalId && mod.style.display !== 'none');

            if (otherOpen.length === 0) {
                backdrop.style.opacity = '0';
                backdrop.style.pointerEvents = 'none';
                setTimeout(() => {
                    if (backdrop.style.opacity === '0') {
                        backdrop.style.display = 'none';
                    }
                }, 300);
                document.body.style.overflow = '';
            }
        }
    } else if (modalId) {
        console.warn(`Attempted to toggle non-existent modal: ${modalId}`);
    }
}


// --- FUNCIONES DE MODALES ---
// Las funciones específicas de módulos (como libros/artículos) se definen en sus respectivas plantillas
// para evitar conflictos entre el módulo de Biblioteca y el Maestro de Artículos ERP.

function fillUserEditModal(id, nombre, apellido, email, telefono) {
    const m = document.getElementById('modal-usuario');
    if (!m) return;
    document.getElementById('user-modal-title').innerText = 'Editar Perfil: ' + nombre;
    document.getElementById('form-usuario').action = '/usuarios/modificar/' + id;
    document.getElementById('user-nombre').value = nombre;
    document.getElementById('user-apellido').value = apellido;
    document.getElementById('user-email').value = email;
    document.getElementById('user-telefono').value = telefono;
    toggleModal('modal-usuario');
}

function openAddUserModal() {
    const m = document.getElementById('modal-usuario');
    if (!m) return;
    document.getElementById('user-modal-title').innerText = 'Registrar Nuevo Usuario';
    document.getElementById('form-usuario').action = '/usuarios/agregar';
    document.getElementById('form-usuario').reset();
    toggleModal('modal-usuario');
}


// Prevent multiple submissions safely
document.addEventListener('submit', function (e) {
    if (e.defaultPrevented) return;
    const form = e.target;
    // Don't apply to search forms or forms with data-no-spinner
    if (form.method.toLowerCase() === 'get' || form.dataset.noSpinner === "true") return;

    const btn = form.querySelector('button[type="submit"]');
    if (btn && !btn.disabled) {
        // Only trigger if form is valid (for HTML5 validation)
        if (form.checkValidity && !form.checkValidity()) return;

        // Password match validation
        const newPass = form.querySelector('input[name="new_password"]');
        const confPass = form.querySelector('input[name="confirm_password"]');
        if (newPass && confPass && newPass.value !== confPass.value) {
            alert("Las contraseñas no coinciden. Por favor verifique.");
            e.preventDefault();
            return;
        }

        btn.dataset.originalText = btn.innerHTML;
        // Small delay to allow the submission to actually start before disabling
        setTimeout(() => {
            btn.disabled = true;
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>...';
        }, 10);
    }
});
/**
 * Setup Book Autocomplete for a specific input field
 * @param {string} inputId - ID of the text input
 * @param {string} hiddenId - ID of the hidden input for ID
 * @param {function} onSelect - Optional callback
 */
function setupBookAutocomplete(inputId, hiddenId, onSelect = null) {
    const input = document.getElementById(inputId);
    const hidden = document.getElementById(hiddenId);
    if (!input) return;

    // Wrap input and create results container
    const wrapper = document.createElement('div');
    wrapper.className = 'autocomplete-container';
    input.parentNode.insertBefore(wrapper, input);
    wrapper.appendChild(input);

    const results = document.createElement('div');
    results.className = 'autocomplete-results';
    wrapper.appendChild(results);

    let debounceTimer;

    input.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        const query = input.value.trim();

        if (query.length < 2) {
            results.style.display = 'none';
            return;
        }

        debounceTimer = setTimeout(async () => {
            try {
                const response = await fetch(`/stock/api/articulos/search?q=${encodeURIComponent(query)}&sid=${window.SID || ''}`);
                const data = await response.json();

                if (data.libros && data.libros.length > 0) {
                    // Build table-based results
                    let html = `
                        <div style="max-height: 400px; overflow-y: auto;">
                            <table style="width: 100%; font-size: 0.85rem; border-collapse: collapse;">
                                <thead style="position: sticky; top: 0; background: #1e293b; z-index: 1;">
                                    <tr>
                                        <th style="padding: 0.5rem; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1);">Artículo</th>
                                        <th style="padding: 0.5rem; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.1);">Disponibles</th>
                                        <th style="padding: 0.5rem; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.1);">Prestados</th>
                                        <th style="padding: 0.5rem; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1);">Próx. Devolución</th>
                                        <th style="padding: 0.5rem; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.1);">Acción</th>
                                    </tr>
                                </thead>
                                <tbody>
                    `;

                    data.libros.forEach(l => {
                        const disponibles = l.disponibles || 0;
                        const prestados = l.prestados || 0;
                        const proxDev = l.proxima_devolucion || '-';
                        const disponibleColor = disponibles > 0 ? '#10b981' : '#ef4444';

                        // Escape quotes for data attributes
                        const safeTitle = (l.nombre || '').replace(/"/g, '&quot;');
                        const safeIsbn = (l.isbn || '').replace(/"/g, '&quot;');

                        html += `
                            <tr class="autocomplete-row" style="cursor: pointer; border-bottom: 1px solid rgba(255,255,255,0.05);" 
                                data-id="${l.id}" data-title="${safeTitle}" data-isbn="${safeIsbn}" data-disponibles="${disponibles}">
                                <td style="padding: 0.75rem;">
                                    <div style="font-weight: 600; color: var(--primary);">${l.nombre}</div>
                                    <div style="font-size: 0.75rem; color: #94a3b8;">ISBN: ${l.isbn} | Autor: ${l.autor}</div>
                                </td>
                                <td style="padding: 0.75rem; text-align: center;">
                                    <span style="color: ${disponibleColor}; font-weight: 600;">${disponibles}</span>
                                </td>
                                <td style="padding: 0.75rem; text-align: center;">
                                    <span style="color: #f59e0b;">${prestados}</span>
                                </td>
                                <td style="padding: 0.75rem; font-size: 0.8rem; color: #cbd5e1;">
                                    ${proxDev}
                                </td>
                                <td style="padding: 0.75rem; text-align: center;">
                                    ${disponibles > 0
                                ? `<button class="btn-select-book" style="padding: 0.25rem 0.75rem; background: var(--primary); color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.8rem;">
                                            <i class="fas fa-check"></i> Seleccionar
                                           </button>`
                                : `<button class="btn-reserve-book" style="padding: 0.25rem 0.75rem; background: #f59e0b; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.8rem;">
                                            <i class="fas fa-bookmark"></i> Reservar
                                           </button>`
                            }
                                </td>
                            </tr>
                        `;
                    });

                    html += '</tbody></table></div>';
                    results.innerHTML = html;
                    results.style.display = 'block';

                    // Selection logic for available books
                    results.querySelectorAll('.btn-select-book').forEach(btn => {
                        btn.onclick = (e) => {
                            e.stopPropagation();
                            const row = btn.closest('.autocomplete-row');
                            input.value = row.dataset.title;
                            hidden.value = row.dataset.id;
                            results.style.display = 'none';
                            if (onSelect) onSelect(row.dataset);
                        };
                    });

                    // Row click for available books
                    results.querySelectorAll('.autocomplete-row').forEach(row => {
                        row.onclick = () => {
                            if (parseInt(row.dataset.disponibles) > 0) {
                                input.value = row.dataset.title;
                                hidden.value = row.dataset.id;
                                results.style.display = 'none';
                                if (onSelect) onSelect(row.dataset);
                            }
                        };
                    });

                    // Reservation logic
                    results.querySelectorAll('.btn-reserve-book').forEach(btn => {
                        btn.onclick = (e) => {
                            e.stopPropagation();
                            const row = btn.closest('.autocomplete-row');
                            reservarArticulo(row.dataset.id, row.dataset.title);
                        };
                    });
                } else {
                    results.innerHTML = '<div class="autocomplete-item text-muted">No se encontraron libros</div>';
                    results.style.display = 'block';
                }
            } catch (e) {
                console.error("Autocomplete error:", e);
            }
        }, 300);
    });

    // Close on blur (delayed to allow click)
    document.addEventListener('click', (e) => {
        if (!wrapper.contains(e.target)) {
            results.style.display = 'none';
        }
    });
}

/**
 * Reserve a book for the selected user
 */
function reservarArticulo(libroId, libroTitulo) {
    // Get selected user from the form
    const usuarioSelect = document.querySelector('select[name="usuario_id"]');
    if (!usuarioSelect || !usuarioSelect.value) {
        alert('Por favor, seleccione primero un usuario para la reserva.');
        return;
    }

    const usuarioId = usuarioSelect.value;
    const usuarioText = usuarioSelect.options[usuarioSelect.selectedIndex].text;

    if (confirm(`¿Reservar "${libroTitulo}" para ${usuarioText}?\n\nSe notificará cuando el libro esté disponible.`)) {
        fetch(`/prestamos/reservar?sid=${window.SID || ''}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRF-Token': document.querySelector('input[name="csrf_token"]')?.value || ''
            },
            body: JSON.stringify({
                libro_id: libroId,
                usuario_id: usuarioId
            })
        })
            .then(res => res.json())
            .then(data => {
                if (data.success) {
                    alert(`✓ Reserva confirmada para ${usuarioText}\n\nSe enviará un email cuando "${libroTitulo}" esté disponible.`);
                    // Close autocomplete
                    document.querySelector('.autocomplete-results').style.display = 'none';
                } else {
                    alert('Error al crear la reserva: ' + (data.error || 'Error desconocido'));
                }
            })
            .catch(err => {
                console.error('Reservation error:', err);
                alert('Error al procesar la reserva');
            });
    }
}

/**
 * Valida un número de CUIT/CUIL argentino siguiendo la regla del Módulo 11
 * @param {string} cuit - El CUIT a validar (con o sin guiones)
 * @returns {boolean} - True si es válido
 */
function validarCUIT(cuit) {
    if (!cuit) return false;

    // Eliminar guiones y dejar solo números
    cuit = cuit.replace(/[^0-9]/g, '');

    // Debe tener exactamente 11 dígitos
    if (cuit.length !== 11) return false;

    const serie = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2];
    let sumatoria = 0;

    // Multiplicación y Sumatoria (primeros 10 dígitos)
    for (let i = 0; i < 10; i++) {
        sumatoria += parseInt(cuit[i]) * serie[i];
    }

    // Módulo 11
    const resto = sumatoria % 11;
    let verificadorCalculado;

    // Cálculo del Verificador
    if (resto === 0) {
        verificadorCalculado = 0;
    } else if (resto === 1) {
        // Casos especiales (generalmente prefijos 23, 33, 34)
        verificadorCalculado = 9;
    } else {
        verificadorCalculado = 11 - resto;
    }

    // Comparación con el undécimo dígito
    return verificadorCalculado === parseInt(cuit[10]);
}
// --- PREMIUM UX HELPERS ---

/**
 * Toast Notification System
 * @param {string} message - Text to display
 * @param {string} type - 'success', 'error', 'warning'
 */
function showToast(message, type = 'success') {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        document.body.appendChild(container);
    }

    const toast = document.createElement('div');
    toast.className = `premium-toast toast-${type}`;

    let icon = 'fa-check-circle';
    if (type === 'error') icon = 'fa-exclamation-circle';
    if (type === 'warning') icon = 'fa-exclamation-triangle';

    toast.innerHTML = `
        <div class="toast-icon"><i class="fas ${icon}"></i></div>
        <div class="toast-content" style="flex: 1; font-size: 0.9rem; font-weight: 500;">${message}</div>
        <div class="toast-close" style="cursor: pointer; opacity: 0.5; transition: 0.2s;" onclick="this.parentElement.remove()">
            <i class="fas fa-times"></i>
        </div>
    `;

    container.appendChild(toast);

    // Auto remove
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(10px)';
        toast.style.transition = '0.4s';
        setTimeout(() => toast.remove(), 400);
    }, 4500);
}

/**
 * Premium Alert System (SOD/SOX Style)
 * @param {string} message - Content of the alert
 * @param {string} type - 'success', 'error', 'warning', 'info'
 * @param {string} title - Optional custom title
 */
function showPremiumAlert(message, type = 'error', customTitle = null) {
    if (typeof Swal === 'undefined') {
        alert(message); // Fallback
        return;
    }

    const cat = type;
    const msg = message;
    const status = (cat === 'danger' || cat === 'error' ? 'error' : (cat.includes('success') ? 'success' : (cat === 'warning' ? 'warning' : 'info')));
    const title = customTitle || (cat === 'danger' || cat === 'error' ? 'ERROR DE OPERACIÓN' : (cat.includes('success') ? 'OPERACIÓN EXITOSA' : (cat === 'warning' ? 'ADVERTENCIA' : 'INFORMACIÓN DEL SISTEMA')));
    const color = (cat === 'danger' || cat === 'error' ? '#ef4444' : (cat.includes('success') ? '#10b981' : (cat === 'warning' ? '#f59e0b' : '#3b82f6')));
    const icon_fa = (cat === 'danger' || cat === 'error' ? 'fa-circle-xmark' : (cat.includes('success') ? 'fa-circle-check' : (cat === 'warning' ? 'fa-triangle-exclamation' : 'fa-circle-info')));

    Swal.fire({
        icon: status,
        title: `<span style="color: ${color}; font-weight: 800; font-size: 1.1rem; text-transform: uppercase;">
                    <i class="fas ${icon_fa}"></i> ${title}
                </span>`,
        html: `
            <div style="border-top: 1px solid rgba(255,255,255,0.1); margin-top: 10px; padding-top: 15px; text-align: left;">
                <div style="background: rgba(0,0,0,0.2); border-left: 4px solid ${color}; padding: 15px; border-radius: 5px; color: #f8fafc; font-size: 0.95rem; user-select: text; line-height: 1.6; word-break: break-word;">
                    ${msg}
                </div>
                <div style="margin-top: 10px; font-size: 0.75rem; color: #94a3b8; text-align: right;">
                    <i class="fas fa-copy"></i> Puede seleccionar y copiar este mensaje para soporte técnico.
                </div>
            </div>`,
        background: '#111827',
        color: '#fff',
        confirmButtonColor: color,
        confirmButtonText: (cat.includes('success') ? 'EXCELENTE' : (cat === 'info' ? 'ENTENDIDO' : 'CERRAR')),
        width: '600px',
        allowOutsideClick: true,
        showCloseButton: true,
        customClass: {
            popup: 'premium-swal-popup',
            title: 'premium-swal-title'
        }
    });
}

/**
 * Enhanced Back Navigation
 */
function goBack() {
    if (window.history.length > 1) {
        window.history.back();
    } else {
        window.location.href = '/';
    }
}

/**
 * Offline/Online Monitoring
 */
window.addEventListener('online', () => showToast('Conexión restablecida', 'success'));
window.addEventListener('offline', () => showToast('Se ha perdido la conexión a internet', 'error'));

/**
 * Specialized Modal for AFIP Errors with action suggestions
 */
function showAFIPErrorModal(message, codes) {
    if (typeof Swal === 'undefined') {
        alert("AFIP REJECTED: " + message);
        return;
    }
    
    const color = '#ef4444'; // Red for errors
    const title = 'RECHAZO DE AFIP';
    
    // Force array if string
    const codesList = Array.isArray(codes) ? codes : [codes];
    
    // Check if we have the "sync" error (10016 - El numero de comprobante no coincide)
    const hasSyncError = codesList.includes('10016') || codesList.includes(10016);
    const syncActionHtml = hasSyncError ? `
        <div style="margin-top: 15px; padding: 12px; background: rgba(234, 179, 8, 0.1); border: 1px dashed #eab308; border-radius: 8px; font-size: 0.85rem; color: #fef08a;">
            <i class="fas fa-sync-alt me-2"></i> <strong>Sugerencia Técnica:</strong> AFIP indica que el número de comprobante no coincide con sus registros.
            <div style="margin-top: 8px; display: flex; justify-content: flex-end;">
                <a href="/configuracion/puntos-venta" class="btn btn-xs btn-warning" style="font-size: 0.7rem; padding: 2px 8px; color: #000; text-decoration: none; border-radius: 4px; font-weight: bold; background: #eab308;">
                    <i class="fas fa-external-link-alt me-1"></i> Ir a Sincronizar Pv
                </a>
            </div>
        </div>
    ` : '';

    Swal.fire({
        icon: 'error',
        title: `<span style="color: ${color}; font-weight: 800; font-size: 1.1rem; text-transform: uppercase;">
                    <i class="fas fa-file-circle-exclamation"></i> ${title}
                </span>`,
        html: `
            <div style="border-top: 1px solid rgba(255,255,255,0.1); margin-top: 10px; padding-top: 15px; text-align: left;">
                <p style="font-size: 0.9rem; color: #94a3b8; margin-bottom: 12px;">El servicio de Facturación Electrónica ha rechazado la solicitud:</p>
                <div style="background: rgba(0,0,0,0.3); border-left: 4px solid ${color}; padding: 15px; border-radius: 5px; color: #f8fafc; font-size: 0.95rem; user-select: text; line-height: 1.6; word-break: break-word;">
                    ${message}
                </div>
                ${syncActionHtml}
                <div style="margin-top: 15px; padding: 8px; background: rgba(255,255,255,0.03); border-radius: 4px; font-size: 0.7rem; color: #64748b;">
                    <strong>Códigos de Error AFIP:</strong> ${codesList.join(', ')}
                </div>
                <div style="margin-top: 15px; font-size: 0.75rem; color: #94a3b8; text-align: center;">
                    <i class="fas fa-info-circle"></i> La transacción de stock y cuenta corriente fue revertida automáticamente.
                </div>
            </div>`,
        background: '#111827',
        color: '#fff',
        confirmButtonColor: color,
        confirmButtonText: 'ENTENDIDO, CORREGIR',
        width: '600px',
        allowOutsideClick: false,
        showCloseButton: true
    });
}
