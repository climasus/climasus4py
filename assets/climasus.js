/* climasus.js — ajustes comportamentais para a documentação */

document.addEventListener('DOMContentLoaded', () => {
  /* Logo → redireciona para a home do portal ClimaSUS */
  const logoBtn = document.querySelector('a.md-header__button.md-logo');
  if (logoBtn) {
    logoBtn.title = 'Ir para ClimaSUS';
    logoBtn.addEventListener('click', (e) => {
      e.preventDefault();
      window.location.href = 'https://climasus.github.io/';
    });
  }
});
