// ========================================
// FUNÇÕES GLOBAIS
// ========================================

function showAlert(message, type = 'error') {
    const alertContainer = document.getElementById('alertContainer');
    if (!alertContainer) return;

    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.innerHTML = `
        <i class="fas fa-${type === 'error' ? 'exclamation-circle' : 'check-circle'}"></i>
        <span>${message}</span>
    `;
    alertContainer.innerHTML = '';
    alertContainer.appendChild(alertDiv);

    if (type === 'success') {
        setTimeout(() => {
            alertDiv.remove();
        }, 3000);
    }
}

function showLoading(show = true) {
    const loadingSection = document.getElementById('loadingSection');
    if (loadingSection) {
        loadingSection.classList.toggle('show', show);
    }
}

// ========================================
// LOGIN PAGE
// ========================================

function showDatabaseSetup() {
    const loginForm = document.getElementById('loginForm');
    const setupForm = document.getElementById('setupForm');
    if (loginForm) loginForm.style.display = 'none';
    if (setupForm) setupForm.style.display = 'block';
}

function showLoginForm() {
    const loginForm = document.getElementById('loginForm');
    const setupForm = document.getElementById('setupForm');
    if (loginForm) loginForm.style.display = 'block';
    if (setupForm) setupForm.style.display = 'none';
    const alertContainer = document.getElementById('alertContainer');
    if (alertContainer) alertContainer.innerHTML = '';
}

// Formulário de Login
document.addEventListener('DOMContentLoaded', function() {
    const formLogin = document.getElementById('formLogin');
    if (formLogin) {
        formLogin.addEventListener('submit', async (e) => {
            e.preventDefault();
            showLoading(true);

            try {
                const response = await fetch('/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        username: document.getElementById('username').value,
                        password: document.getElementById('password').value
                    })
                });

                const data = await response.json();
                showLoading(false);

                if (data.success) {
                    showAlert('Login realizado com sucesso!', 'success');
                    setTimeout(() => {
                        window.location.href = data.redirect;
                    }, 1500);
                } else {
                    showAlert(data.message || 'Erro ao fazer login', 'error');
                }
            } catch (error) {
                showLoading(false);
                showAlert('Erro na conexão: ' + error.message, 'error');
            }
        });
    }

    // Formulário de Configuração do Banco
    const formSetup = document.getElementById('formSetup');
    if (formSetup) {
        formSetup.addEventListener('submit', async (e) => {
            e.preventDefault();
            showLoading(true);

            try {
                const response = await fetch('/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        setup: true,
                        server: document.getElementById('server').value,
                        database: document.getElementById('database').value,
                        username: document.getElementById('db_username').value,
                        password: document.getElementById('db_password').value
                    })
                });

                const data = await response.json();
                showLoading(false);

                if (data.success) {
                    showAlert(data.message, 'success');
                    setTimeout(() => {
                        showLoginForm();
                    }, 1500);
                } else {
                    showAlert(data.message || 'Erro ao configurar banco', 'error');
                }
            } catch (error) {
                showLoading(false);
                showAlert('Erro na conexão: ' + error.message, 'error');
            }
        });
    }

    // Focus no primeiro input
    const username = document.getElementById('username');
    if (username) username.focus();
});

// ========================================
// DASHBOARD
// ========================================

const vendedoresData = window.vendedoresData || [];
const gruposData = window.gruposData || [];

function updateFilterOptions() {
    const tipo = document.getElementById('filtro_tipo').value;
    const filterVendedorGroup = document.getElementById('filterVendedorGroup');
    const filterGrupoGroup = document.getElementById('filterGrupoGroup');
    const filterClienteGroup = document.getElementById('filterClienteGroup');

    // Limpar exibição anterior
    if (filterVendedorGroup) filterVendedorGroup.style.display = 'none';
    if (filterGrupoGroup) filterGrupoGroup.style.display = 'none';
    if (filterClienteGroup) filterClienteGroup.style.display = 'none';

    if (tipo === 'todos') {
        // Sem filtro adicional
        return;
    } else if (tipo === 'cliente') {
        // Mostrar campo de texto para busca
        if (filterClienteGroup) filterClienteGroup.style.display = 'flex';
    } else if (tipo === 'vendedor') {
        // Carregar vendedores
        if (filterVendedorGroup) filterVendedorGroup.style.display = 'flex';
        loadVendedores();
    } else if (tipo === 'grupo') {
        // Carregar grupos
        if (filterGrupoGroup) filterGrupoGroup.style.display = 'flex';
        loadGrupos();
    }
}

function loadVendedores() {


