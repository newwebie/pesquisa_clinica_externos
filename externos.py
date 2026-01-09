"""
Sistema de Avaliação do Gerente Médico
Portal externo para avaliação de desvios de estudos clínicos
"""

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# =========================
# Internacionalização (i18n)
# =========================

TRANSLATIONS = {
    "pt": {
        # Campos do formulário
        "Desvio": "Desvio",
        "Status": "Status",
        "Formulário": "Formulário",
        "Identificação do Desvio": "Identificação do Desvio",
        "Centro": "Centro",
        "Data do ocorrido": "Data do ocorrido",
        "Participante": "Participante",
        "Visita": "Visita",
        "Descrição do desvio": "Descrição do desvio",
        "Causa Raiz": "Causa Raiz",
        "Ação Corretiva": "Ação Corretiva",
        "Importância": "Importância",
        "Data de identificação": "Data de identificação",
        "Categoria": "Categoria",
        "Subcategoria": "Subcategoria",
        "Código": "Código",
        "Recorrência": "Recorrência",
        "N° Desvio Ocorrência Prévia": "N° Desvio Ocorrência Prévia",
        "Escopo": "Escopo",
        "Prazo de Escalonamento": "Prazo de Escalonamento",
        "Data de escalonamento": "Data de escalonamento",
        "Atendeu os prazos de reporte?": "Atendeu os prazos de reporte?",
        "Avaliação do Gerente Médico": "Avaliação do Gerente Médico",
        "Avaliação do Investigador Principal": "Avaliação do Investigador Principal",
        "Formulário Arquivado (ISF e TMF)?": "Formulário Arquivado (ISF e TMF)?",
        "Data de Submissão ao CEP": "Data de Submissão ao CEP",
        "Data de finalização": "Data de finalização",
        "Observação": "Observação",
        "Ação Preventiva": "Ação Preventiva",
        "ID do Desvio": "ID do Desvio",
        "Última Atualização": "Última Atualização",
        "Motivo": "Motivo",
        # Interface
        "Portal do Gerente Médico": "Portal do Gerente Médico",
        "Informe seu e-mail cadastrado para acessar o sistema de avaliação de desvios.": "Informe seu e-mail cadastrado para acessar o sistema de avaliação de desvios.",
        "E-mail": "E-mail",
        "Entrar": "Entrar",
        "Por favor, informe um e-mail.": "Por favor, informe um e-mail.",
        "Verificando credenciais...": "Verificando credenciais...",
        "E-mail não cadastrado como Gerente Médico. Entre em contato com o administrador.": "E-mail não cadastrado como Gerente Médico. Entre em contato com o administrador.",
        "Bem-vindo(a)": "Bem-vindo(a)",
        "Erro ao autenticar": "Erro ao autenticar",
        "Meus Estudos": "Meus Estudos",
        "Selecione um estudo para avaliar os desvios": "Selecione um estudo para avaliar os desvios",
        "Carregando estudos...": "Carregando estudos...",
        "Você não está alocado em nenhum estudo ativo.": "Você não está alocado em nenhum estudo ativo.",
        "Entre em contato com o Gerente de Projetos para ser alocado a um estudo.": "Entre em contato com o Gerente de Projetos para ser alocado a um estudo.",
        "Estudos": "Estudos",
        "Desvios Pendentes": "Desvios Pendentes",
        "Estudos com Pendência": "Estudos com Pendência",
        "pendência(s)": "pendência(s)",
        "Sem pendências": "Sem pendências",
        "Acessar": "Acessar",
        "Filtrar por status": "Filtrar por status",
        "Pendentes": "Pendentes",
        "Todos": "Todos",
        "Novo": "Novo",
        "Modificado": "Modificado",
        "Avaliado": "Avaliado",
        "Atualizar": "Atualizar",
        "Carregando desvios...": "Carregando desvios...",
        "Nenhum desvio pendente de avaliação!": "Nenhum desvio pendente de avaliação!",
        "Nenhum desvio encontrado com o filtro selecionado.": "Nenhum desvio encontrado com o filtro selecionado.",
        "desvio(s) encontrado(s)": "desvio(s) encontrado(s)",
        "Descrição": "Descrição",
        "Selecione um desvio para avaliar": "Selecione um desvio para avaliar",
        "Selecione o desvio:": "Selecione o desvio:",
        "Selecione o ID do desvio...": "Selecione o ID do desvio...",
        "Selecione um desvio na lista acima para visualizar os detalhes e realizar a avaliação.": "Selecione um desvio na lista acima para visualizar os detalhes e realizar a avaliação.",
        "Detalhes do Desvio": "Detalhes do Desvio",
        "Sua Avaliação": "Sua Avaliação",
        "Digite sua avaliação sobre este desvio...": "Digite sua avaliação sobre este desvio...",
        "Salvar Avaliação": "Salvar Avaliação",
        "Por favor, preencha a avaliação antes de salvar.": "Por favor, preencha a avaliação antes de salvar.",
        "Salvando avaliação...": "Salvando avaliação...",
        "Avaliação salva com sucesso!": "Avaliação salva com sucesso!",
        "Este desvio foi modificado por outra pessoa. Clique em 'Atualizar' para ver a versão mais recente.": "Este desvio foi modificado por outra pessoa. Clique em 'Atualizar' para ver a versão mais recente.",
        "Erro ao salvar": "Erro ao salvar",
        "Gerente Médico": "Gerente Médico",
        "Patrocinador": "Patrocinador",
        "Estudo Atual": "Estudo Atual",
        "Trocar Estudo": "Trocar Estudo",
        "Sair": "Sair",
        "Portal Gerente Médico": "Portal Gerente Médico",
        "Idioma": "Idioma",
    },
    "en": {
        # Campos do formulário
        "Desvio": "Deviation",
        "Status": "Status",
        "Formulário": "Form",
        "Identificação do Desvio": "Deviation Identification Number",
        "Centro": "Site",
        "Data do ocorrido": "Date of occurrence",
        "Participante": "Subject",
        "Visita": "Visit",
        "Descrição do desvio": "Description of the deviation",
        "Causa Raiz": "Root cause",
        "Ação Corretiva": "Corrective Action",
        "Importância": "Importance",
        "Data de identificação": "Identification Date",
        "Categoria": "Category",
        "Subcategoria": "Subcategory",
        "Código": "Code",
        "Recorrência": "Recurrence",
        "N° Desvio Ocorrência Prévia": "Deviation Number Prior Occurrence",
        "Escopo": "Scope",
        "Prazo de Escalonamento": "Escalation Deadline",
        "Data de escalonamento": "Escalation Date",
        "Atendeu os prazos de reporte?": "Did you meet the reporting deadlines?",
        "Avaliação do Gerente Médico": "Medical Manager Evaluation",
        "Avaliação do Investigador Principal": "Evaluation of the Principal Investigator",
        "Formulário Arquivado (ISF e TMF)?": "Archived form (ISF and TMF)?",
        "Data de Submissão ao CEP": "Submission Date to the EC",
        "Data de finalização": "Completion date",
        "Observação": "Observation",
        "Ação Preventiva": "Preventive Action",
        "ID do Desvio": "Deviation ID",
        "Última Atualização": "Last Update",
        "Motivo": "Reason",
        # Interface
        "Portal do Gerente Médico": "Medical Manager Portal",
        "Informe seu e-mail cadastrado para acessar o sistema de avaliação de desvios.": "Enter your registered email to access the deviation evaluation system.",
        "E-mail": "Email",
        "Entrar": "Login",
        "Por favor, informe um e-mail.": "Please enter an email.",
        "Verificando credenciais...": "Verifying credentials...",
        "E-mail não cadastrado como Gerente Médico. Entre em contato com o administrador.": "Email not registered as Medical Manager. Contact the administrator.",
        "Bem-vindo(a)": "Welcome",
        "Erro ao autenticar": "Authentication error",
        "Meus Estudos": "My Studies",
        "Selecione um estudo para avaliar os desvios": "Select a study to evaluate the deviations",
        "Carregando estudos...": "Loading studies...",
        "Você não está alocado em nenhum estudo ativo.": "You are not assigned to any active study.",
        "Entre em contato com o Gerente de Projetos para ser alocado a um estudo.": "Contact the Project Manager to be assigned to a study.",
        "Estudos": "Studies",
        "Desvios Pendentes": "Pending Deviations",
        "Estudos com Pendência": "Studies with Pending Items",
        "pendência(s)": "pending item(s)",
        "Sem pendências": "No pending items",
        "Acessar": "Access",
        "Filtrar por status": "Filter by status",
        "Pendentes": "Pending",
        "Todos": "All",
        "Novo": "New",
        "Modificado": "Modified",
        "Avaliado": "Evaluated",
        "Atualizar": "Refresh",
        "Carregando desvios...": "Loading deviations...",
        "Nenhum desvio pendente de avaliação!": "No deviations pending evaluation!",
        "Nenhum desvio encontrado com o filtro selecionado.": "No deviations found with the selected filter.",
        "desvio(s) encontrado(s)": "deviation(s) found",
        "Descrição": "Description",
        "Selecione um desvio para avaliar": "Select a deviation to evaluate",
        "Selecione o desvio:": "Select the deviation:",
        "Selecione o ID do desvio...": "Select the deviation ID...",
        "Selecione um desvio na lista acima para visualizar os detalhes e realizar a avaliação.": "Select a deviation from the list above to view details and perform the evaluation.",
        "Detalhes do Desvio": "Deviation Details",
        "Sua Avaliação": "Your Evaluation",
        "Digite sua avaliação sobre este desvio...": "Enter your evaluation of this deviation...",
        "Salvar Avaliação": "Save Evaluation",
        "Por favor, preencha a avaliação antes de salvar.": "Please fill in the evaluation before saving.",
        "Salvando avaliação...": "Saving evaluation...",
        "Avaliação salva com sucesso!": "Evaluation saved successfully!",
        "Este desvio foi modificado por outra pessoa. Clique em 'Atualizar' para ver a versão mais recente.": "This deviation was modified by someone else. Click 'Refresh' to see the latest version.",
        "Erro ao salvar": "Error saving",
        "Gerente Médico": "Medical Manager",
        "Patrocinador": "Sponsor",
        "Estudo Atual": "Current Study",
        "Trocar Estudo": "Change Study",
        "Sair": "Logout",
        "Portal Gerente Médico": "Medical Manager Portal",
        "Idioma": "Language",
    },
}


def t(key: str) -> str:
    """Retorna a tradução de uma chave baseado no idioma selecionado"""
    lang = st.session_state.get("language", "pt")
    return TRANSLATIONS.get(lang, TRANSLATIONS["pt"]).get(key, key)


# =========================
# Config / Conexão com Banco
# =========================

def get_connection():
    """Cria conexão com o banco PostgreSQL usando secrets.toml"""
    db = st.secrets["postgres"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["database"],
        user=db["user"],
        password=db["password"],
    )


# =========================
# Envio de Email
# =========================

def buscar_emails_monitores_do_estudo(estudo_id: int, excluir_email: str = None):
    """
    Busca os emails dos monitores do estudo.
    Opcionalmente exclui um email específico (ex: o gerente médico que fez a avaliação).
    Retorna uma lista de emails únicos.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Busca apenas monitores do estudo
        cursor.execute(
            """
            SELECT DISTINCT monitor_email
            FROM estudo_monitores
            WHERE estudo_id = %s AND monitor_email IS NOT NULL AND monitor_email != ''
            """,
            (estudo_id,),
        )
        monitores = [row['monitor_email'].lower() for row in cursor.fetchall() if row['monitor_email']]

        # Remove o email a ser excluído (se fornecido)
        if excluir_email:
            excluir_email_lower = excluir_email.lower()
            monitores = [e for e in monitores if e != excluir_email_lower]

        return monitores

    except Exception as e:
        print(f"Erro ao buscar destinatários: {e}")
        return []
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def enviar_email_avaliacao(
    estudo_id: int,
    estudo_codigo: str,
    estudo_nome: str,
    numero_desvio: int,
    avaliacao: str,
    gerente_nome: str,
    gerente_email: str
):
    """
    Envia email notificando sobre a avaliação do gerente médico.
    Envia apenas para monitores do estudo (excluindo o gerente médico).
    """
    try:
        # Buscar destinatários (apenas monitores, excluindo o gerente médico)
        destinatarios = buscar_emails_monitores_do_estudo(estudo_id, excluir_email=gerente_email)
        if not destinatarios:
            print("Nenhum destinatário encontrado para enviar email")
            return True  # Não é erro, apenas não há destinatários

        # Configurações de email
        email_config = st.secrets.get("email", {})
        smtp_server = email_config.get("smtp_server")
        smtp_port = email_config.get("smtp_port", 587)
        sender = email_config.get("sender")
        password = email_config.get("password")

        if not all([smtp_server, sender, password]):
            print("Configurações de email incompletas no secrets.toml")
            return False

        # Formatar data/hora atual
        data_atual = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y às %H:%M")

        # Assunto do email
        assunto = f"[Avaliação GM] {estudo_codigo} - Desvio {numero_desvio}"

        # Corpo do email em HTML (layout azul)
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
        </head>
        <body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f5f5f5;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f5f5f5; padding: 30px 0;">
                <tr>
                    <td align="center">
                        <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); overflow: hidden;">

                            <!-- Header -->
                            <tr>
                                <td style="background: linear-gradient(135deg, #1976D2 0%, #1565C0 100%); padding: 30px 40px; text-align: center;">
                                    <h1 style="margin: 0; color: #ffffff; font-size: 24px; font-weight: 600;">
                                        🩺 Avaliação do Gerente Médico
                                    </h1>
                                    <p style="margin: 10px 0 0; color: rgba(255,255,255,0.9); font-size: 14px;">
                                        Portal Pesquisa Clínica
                                    </p>
                                </td>
                            </tr>

                            <!-- Content -->
                            <tr>
                                <td style="padding: 40px;">

                                    <!-- Info Cards -->
                                    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 30px;">
                                        <tr>
                                            <td style="padding: 15px; background-color: #f8f9fa; border-radius: 8px; border-left: 4px solid #1976D2;">
                                                <table width="100%" cellpadding="0" cellspacing="0">
                                                    <tr>
                                                        <td width="50%" style="padding: 8px 0;">
                                                            <span style="color: #666; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Estudo</span><br>
                                                            <span style="color: #333; font-size: 16px; font-weight: 600;">{estudo_codigo}</span><br>
                                                            <span style="color: #666; font-size: 13px;">{estudo_nome}</span>
                                                        </td>
                                                        <td width="50%" style="padding: 8px 0; text-align: right;">
                                                            <span style="color: #666; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Desvio ID</span><br>
                                                            <span style="color: #1976D2; font-size: 24px; font-weight: 700;">{numero_desvio}</span>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
                                    </table>

                                    <!-- Meta Info -->
                                    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 25px;">
                                        <tr>
                                            <td style="padding: 10px 0; border-bottom: 1px solid #eee;">
                                                <span style="color: #999; font-size: 13px;">🩺 Avaliado por:</span>
                                                <span style="color: #333; font-size: 14px; font-weight: 500; margin-left: 10px;">{gerente_nome}</span>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="padding: 10px 0;">
                                                <span style="color: #999; font-size: 13px;">📅 Data/Hora:</span>
                                                <span style="color: #333; font-size: 14px; font-weight: 500; margin-left: 10px;">{data_atual}</span>
                                            </td>
                                        </tr>
                                    </table>

                                    <!-- Avaliação -->
                                    <h3 style="margin: 0 0 15px; color: #333; font-size: 16px; font-weight: 600;">
                                        📋 Avaliação do Gerente Médico
                                    </h3>
                                    <div style="background-color: #e3f2fd; border-radius: 8px; padding: 20px; border-left: 4px solid #1976D2;">
                                        <p style="margin: 0; color: #333; font-size: 14px; line-height: 1.6; white-space: pre-wrap;">{avaliacao or '-'}</p>
                                    </div>

                                </td>
                            </tr>

                            <!-- Footer -->
                            <tr>
                                <td style="background-color: #f8f9fa; padding: 25px 40px; text-align: center; border-top: 1px solid #eee;">
                                    <p style="margin: 0; color: #999; font-size: 12px;">
                                        Este é um email automático do sistema Portal Pesquisa Clínica.<br>
                                        Por favor, não responda a este email.
                                    </p>
                                    <p style="margin: 15px 0 0; color: #1976D2; font-size: 11px; font-weight: 600;">
                                        © {datetime.now().year} Synvia
                                    </p>
                                </td>
                            </tr>

                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

        # Enviar para cada destinatário
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)

            for destinatario in destinatarios:
                msg = MIMEMultipart('alternative')
                msg['Subject'] = assunto
                msg['From'] = sender
                msg['To'] = destinatario

                msg.attach(MIMEText(html_body, 'html'))

                server.sendmail(sender, destinatario, msg.as_string())

        print(f"Email de avaliação GM enviado para {len(destinatarios)} destinatário(s)")
        return True

    except Exception as e:
        print(f"Erro ao enviar email: {e}")
        return False


# =========================
# Autenticação do Gerente Médico
# =========================

def login_screen():
    """Tela de login - verifica email na tabela gerentes_medicos"""
    # Seletor de idioma na tela de login
    col_lang, _ = st.columns([1, 4])
    with col_lang:
        idioma_opcoes = {"🌐 Português": "pt", "🌐 English": "en"}
        idioma_atual = st.session_state.get("language", "pt")
        opcao_atual = "🌐 Português" if idioma_atual == "pt" else "🌐 English"
        novo_idioma = st.selectbox(
            "Idioma",
            options=list(idioma_opcoes.keys()),
            index=list(idioma_opcoes.keys()).index(opcao_atual),
            key="login_language_selector",
            label_visibility="collapsed",
        )
        if idioma_opcoes[novo_idioma] != idioma_atual:
            st.session_state["language"] = idioma_opcoes[novo_idioma]
            st.rerun()

    st.title(f"🔐 {t('Portal do Gerente Médico')}")
    st.write(t("Informe seu e-mail cadastrado para acessar o sistema de avaliação de desvios."))

    email = st.text_input(t("E-mail"), placeholder="seu.email@empresa.com")

    if st.button(t("Entrar"), type="primary"):
        if not email:
            st.warning(t("Por favor, informe um e-mail."))
            return

        with st.spinner(t("Verificando credenciais...")):
            try:
                conn = get_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                # Busca por e-mail na tabela gerentes_medicos (case-insensitive)
                cursor.execute(
                    """
                    SELECT id, nome, email, patrocinador
                    FROM gerentes_medicos
                    WHERE LOWER(email) = LOWER(%s)
                    """,
                    (email.strip(),),
                )
                gerente = cursor.fetchone()

                if not gerente:
                    st.error(t("E-mail não cadastrado como Gerente Médico. Entre em contato com o administrador."))
                    return

                # Guarda informações na sessão
                st.session_state["is_authenticated"] = True
                st.session_state["gerente_id"] = gerente["id"]
                st.session_state["gerente_nome"] = gerente["nome"]
                st.session_state["gerente_email"] = gerente["email"]
                st.session_state["gerente_patrocinador"] = gerente["patrocinador"]

                st.success(f"{t('Bem-vindo(a)')}, {gerente['nome']}!")
                st.rerun()

            except Exception as e:
                st.error(f"{t('Erro ao autenticar')}: {e}")
            finally:
                try:
                    cursor.close()
                    conn.close()
                except Exception:
                    pass


# =========================
# Seleção de Estudo
# =========================

@st.cache_data(ttl=300, show_spinner=False)
def carregar_estudos_do_gerente(_email: str):
    """Carrega lista de estudos ativos alocados ao gerente médico logado com contagem de pendências"""
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Carrega estudos com contagem de desvios pendentes
        cursor.execute(
            """
            SELECT
                e.id,
                e.codigo,
                e.nome,
                COUNT(CASE WHEN d.status != 'Avaliado' THEN 1 END) AS pendentes
            FROM estudos e
            INNER JOIN estudo_gerente_medico egm ON e.id = egm.estudo_id
            INNER JOIN gerentes_medicos gm ON gm.id = egm.gerente_medico_id
            LEFT JOIN desvios d ON d.estudo_id = e.id AND d.deleted_at IS NULL
            WHERE LOWER(gm.email) = LOWER(%s)
              AND e.status = 'ativo'
            GROUP BY e.id, e.codigo, e.nome
            ORDER BY pendentes DESC, e.nome
            """,
            (_email,),
        )
        estudos = cursor.fetchall()
        return estudos

    except Exception as e:
        return []
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


@st.cache_data(ttl=300, show_spinner=False)
def carregar_metricas_gerente(_email: str):
    """Carrega métricas gerais do gerente médico"""
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            """
            SELECT
                COUNT(DISTINCT e.id) AS total_estudos,
                COUNT(CASE WHEN d.status != 'Avaliado' THEN 1 END) AS total_pendentes,
                COUNT(DISTINCT CASE WHEN d.status != 'Avaliado' THEN e.id END) AS estudos_com_pendencia
            FROM estudos e
            INNER JOIN estudo_gerente_medico egm ON e.id = egm.estudo_id
            INNER JOIN gerentes_medicos gm ON gm.id = egm.gerente_medico_id
            LEFT JOIN desvios d ON d.estudo_id = e.id AND d.deleted_at IS NULL
            WHERE LOWER(gm.email) = LOWER(%s)
              AND e.status = 'ativo'
            """,
            (_email,),
        )
        metricas = cursor.fetchone()
        return metricas

    except Exception as e:
        return {"total_estudos": 0, "total_pendentes": 0, "estudos_com_pendencia": 0}
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def selecao_estudo_screen():
    """Tela de seleção de estudo com cards em grid e métricas"""
    st.title(f"📚 {t('Meus Estudos')}")
    st.caption(t("Selecione um estudo para avaliar os desvios"))

    email = st.session_state["gerente_email"]

    # Carregar dados (com cache e spinner)
    with st.spinner(t("Carregando estudos...")):
        estudos = carregar_estudos_do_gerente(email)
        metricas = carregar_metricas_gerente(email)

    if not estudos:
        st.warning(t("Você não está alocado em nenhum estudo ativo."))
        st.info(t("Entre em contato com o Gerente de Projetos para ser alocado a um estudo."))
        return

    # Painel de métricas no topo com containers
    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.metric(
                label=t("Estudos"),
                value=metricas["total_estudos"],
            )

    with col2:
        with st.container(border=True):
            st.metric(
                label=t("Desvios Pendentes"),
                value=metricas["total_pendentes"],
            )

    with col3:
        with st.container(border=True):
            st.metric(
                label=t("Estudos com Pendência"),
                value=metricas["estudos_com_pendencia"],
            )

    st.markdown("")
    st.markdown("")
    st.markdown("")

    # Grid de cards (3 colunas)
    cols_per_row = 3
    df = pd.DataFrame(estudos)
    rows = [df.iloc[i:i + cols_per_row] for i in range(0, len(df), cols_per_row)]

    for row_data in rows:
        cols = st.columns(cols_per_row)
        for idx, (_, estudo) in enumerate(row_data.iterrows()):
            with cols[idx]:
                with st.container(border=True):
                    pendentes = estudo['pendentes']

                    # Código do estudo em destaque
                    st.subheader(estudo['codigo'])
                    st.caption(estudo['nome'])

                    # Indicador de status com emoji
                    if pendentes > 0:
                        st.write(f"🔴 {pendentes} {t('pendência(s)')}")
                    else:
                        st.write(f"🟢 {t('Sem pendências')}")

                    if st.button(
                        t("Acessar"),
                        key=f"entrar_{estudo['id']}",
                        use_container_width=True,
                        type="primary" if pendentes > 0 else "secondary"
                    ):
                        st.session_state["estudo_id"] = estudo['id']
                        st.session_state["estudo_codigo"] = estudo['codigo']
                        st.session_state["estudo_nome"] = estudo['nome']
                        st.rerun()


# =========================
# Lista de Desvios
# =========================

@st.cache_data(ttl=300, show_spinner=False)
def carregar_desvios_do_estudo(estudo_id: int, filtro_status: str = "Pendentes"):
    """Carrega desvios do estudo selecionado"""
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Query base - todos os campos do formulário (exclui soft deleted)
        query = """
            SELECT
                *,
                xmin AS row_version
            FROM desvios
            WHERE estudo_id = %s
              AND deleted_at IS NULL
        """

        params = [estudo_id]

        # Aplicar filtro de status
        if filtro_status == "Pendentes":
            query += " AND status != 'Avaliado'"
        elif filtro_status != "Todos":
            query += " AND status = %s"
            params.append(filtro_status)

        query += " ORDER BY numero_desvio_estudo DESC"

        cursor.execute(query, params)
        desvios = cursor.fetchall()
        return desvios

    except Exception:
        return []
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def limpar_cache():
    """Limpa o cache de todas as consultas"""
    carregar_estudos_do_gerente.clear()
    carregar_metricas_gerente.clear()
    carregar_desvios_do_estudo.clear()


def salvar_avaliacao(desvio: dict, estudo_id: int, avaliacao: str, row_version, valor_antigo: str, status_antigo: str):
    """
    Salva a avaliação do gerente médico e atualiza o status para 'Avaliado'.
    Registra as alterações no log e envia email de notificação.
    """
    gerente_nome = st.session_state["gerente_nome"]
    desvio_id = desvio["id"]

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. Atualizar o desvio com controle de concorrência (xmin)
        cursor.execute(
            """
            UPDATE desvios
            SET
                avaliacao_gerente_medico = %s,
                status = 'Avaliado',
                atualizado_por = %s,
                data_atualizacao = NOW()
            WHERE id = %s
              AND xmin = %s::xid
            """,
            (avaliacao, gerente_nome, desvio_id, row_version),
        )

        if cursor.rowcount == 0:
            conn.rollback()
            return False, "conflito"

        # 2. Registrar no log - alteração da avaliação
        cursor.execute(
            """
            INSERT INTO desvios_log (desvio_id, estudo_id, usuario, campo, valor_antigo, valor_novo, data_alteracao)
            VALUES (%s, %s, %s, 'avaliacao_gerente_medico', %s, %s, NOW())
            """,
            (desvio_id, estudo_id, gerente_nome, valor_antigo or '', avaliacao),
        )

        # 3. Registrar no log - alteração do status (se mudou)
        if status_antigo != 'Avaliado':
            cursor.execute(
                """
                INSERT INTO desvios_log (desvio_id, estudo_id, usuario, campo, valor_antigo, valor_novo, data_alteracao)
                VALUES (%s, %s, %s, 'status', %s, 'Avaliado', NOW())
                """,
                (desvio_id, estudo_id, gerente_nome, status_antigo),
            )

        conn.commit()

        # Limpar cache após salvar com sucesso
        limpar_cache()

        # 4. Enviar email de notificação (em background, não bloqueia)
        try:
            enviar_email_avaliacao(
                estudo_id=estudo_id,
                estudo_codigo=st.session_state.get("estudo_codigo", ""),
                estudo_nome=st.session_state.get("estudo_nome", ""),
                numero_desvio=desvio.get("numero_desvio_estudo", 0),
                avaliacao=avaliacao,
                gerente_nome=gerente_nome,
                gerente_email=st.session_state.get("gerente_email", "")
            )
        except Exception as email_error:
            # Não falha a operação se o email não for enviado
            print(f"Aviso: Email não enviado - {email_error}")

        return True, "sucesso"

    except Exception as e:
        return False, str(e)
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def get_campo_traduzido(desvio: dict, campo: str) -> str:
    """
    Retorna o valor do campo traduzido baseado no idioma selecionado.
    Para campos de selectbox que possuem versão _en, usa a coluna apropriada.

    Campos com tradução: status, formulario_status, importancia, recorrencia,
                         escopo, atendeu_prazos_report, formulario_arquivado
    """
    lang = st.session_state.get("language", "pt")

    if lang == "en":
        campo_en = f"{campo}_en"
        if campo_en in desvio and desvio.get(campo_en):
            return desvio.get(campo_en) or '-'

    return desvio.get(campo) or '-'


def formatar_data(data_raw):
    """Formata data para exibição no formato brasileiro"""
    if not data_raw:
        return '-'
    # Se for datetime (tem hora)
    if isinstance(data_raw, datetime):
        utc_minus_3 = timezone(timedelta(hours=-3))
        if data_raw.tzinfo is None:
            data_utc = data_raw.replace(tzinfo=timezone.utc)
            data_brasilia = data_utc.astimezone(utc_minus_3)
        else:
            data_brasilia = data_raw.astimezone(utc_minus_3)
        return data_brasilia.strftime("%d/%m/%Y %H:%M")
    # Se for date (só data, sem hora)
    if hasattr(data_raw, 'strftime'):
        return data_raw.strftime("%d/%m/%Y")
    return str(data_raw)


def exibir_detalhes_desvio(desvio):
    """Exibe os detalhes do desvio em formato somente leitura - TODOS os campos"""
    desvio_id = desvio['id']  # Para keys únicas

    # === SEÇÃO 1: Informações Básicas ===
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container(border=True):
            st.caption(t("ID do Desvio"))
            st.markdown(f"**#{desvio['numero_desvio_estudo']}**")

    with col2:
        with st.container(border=True):
            st.caption(t("Status"))
            status = get_campo_traduzido(desvio, 'status')
            if status.lower() in ['avaliado', 'evaluated']:
                st.success(status)
            else:
                st.markdown(f"**{status}**")

    with col3:
        with st.container(border=True):
            st.caption(t("Formulário"))
            st.markdown(f"**{get_campo_traduzido(desvio, 'formulario_status')}**")

    with col4:
        with st.container(border=True):
            st.caption(t("Importância"))
            importancia = get_campo_traduzido(desvio, 'importancia')
            if importancia.lower() in ['major', 'maior']:
                st.error(importancia)
            else:
                st.markdown(f"**{importancia}**")

    st.write("")

    # === SEÇÃO 2: Identificação ===
    col5, col6, col7 = st.columns(3)

    with col5:
        with st.container(border=True):
            st.caption(t("Identificação do Desvio"))
            st.markdown(f"**{desvio.get('identificacao_desvio') or '-'}**")

    with col6:
        with st.container(border=True):
            st.caption(t("Centro"))
            st.markdown(f"**{desvio.get('centro') or '-'}**")

    with col7:
        with st.container(border=True):
            st.caption(t("Data do ocorrido"))
            st.markdown(f"**{formatar_data(desvio.get('data_ocorrido'))}**")

    st.write("")

    # === SEÇÃO 3: Participante e Visita ===
    col8, col9, col10 = st.columns(3)

    with col8:
        with st.container(border=True):
            st.caption(t("Participante"))
            st.markdown(f"**{desvio.get('participante') or '-'}**")

    with col9:
        with st.container(border=True):
            st.caption(t("Visita"))
            st.markdown(f"**{desvio.get('visita') or '-'}**")

    with col10:
        with st.container(border=True):
            st.caption(t("Data de identificação"))
            st.markdown(f"**{desvio.get('data_identificacao_texto') or '-'}**")

    st.write("")

    # === SEÇÃO 4: Categoria e Código ===
    col11, col12, col13 = st.columns(3)

    with col11:
        with st.container(border=True):
            st.caption(t("Categoria"))
            st.markdown(f"**{desvio.get('categoria') or '-'}**")

    with col12:
        with st.container(border=True):
            st.caption(t("Subcategoria"))
            st.markdown(f"**{desvio.get('subcategoria') or '-'}**")

    with col13:
        with st.container(border=True):
            st.caption(t("Código"))
            st.markdown(f"**{desvio.get('codigo') or '-'}**")

    st.write("")

    # === SEÇÃO 5: Recorrência e Escopo ===
    col14, col15, col16 = st.columns(3)

    with col14:
        with st.container(border=True):
            st.caption(t("Recorrência"))
            st.markdown(f"**{get_campo_traduzido(desvio, 'recorrencia')}**")

    with col15:
        with st.container(border=True):
            st.caption(t("N° Desvio Ocorrência Prévia"))
            st.markdown(f"**{desvio.get('num_ocorrencia_previa') or '-'}**")

    with col16:
        with st.container(border=True):
            st.caption(t("Escopo"))
            st.markdown(f"**{get_campo_traduzido(desvio, 'escopo')}**")

    st.write("")

    # === SEÇÃO 6: Escalonamento ===
    col17, col18, col19 = st.columns(3)

    with col17:
        with st.container(border=True):
            st.caption(t("Prazo de Escalonamento"))
            st.markdown(f"**{formatar_data(desvio.get('prazo_escalonamento'))}**")

    with col18:
        with st.container(border=True):
            st.caption(t("Data de escalonamento"))
            st.markdown(f"**{formatar_data(desvio.get('data_escalonamento'))}**")

    with col19:
        with st.container(border=True):
            st.caption(t("Atendeu os prazos de reporte?"))
            st.markdown(f"**{get_campo_traduzido(desvio, 'atendeu_prazos_report')}**")

    st.divider()

    # === SEÇÃO 7: Descrição do Desvio ===
    st.markdown(f"**{t('Descrição do desvio')}**")
    st.text_area(
        t("Descrição do desvio"),
        value=desvio.get('descricao_desvio') or '-',
        disabled=True,
        height=100,
        key=f"desc_readonly_{desvio_id}",
        label_visibility="collapsed"
    )

    st.write("")

    # === SEÇÃO 8: Motivo (novo campo) ===
    st.markdown(f"**{t('Motivo')}**")
    st.text_area(
        t("Motivo"),
        value=desvio.get('motivo') or '-',
        disabled=True,
        height=100,
        key=f"motivo_readonly_{desvio_id}",
        label_visibility="collapsed"
    )

    st.write("")

    # === SEÇÃO 9: Causa Raiz e Ações ===
    col_causa, col_corr = st.columns(2)

    with col_causa:
        st.markdown(f"**{t('Causa Raiz')}**")
        st.text_area(
            t("Causa Raiz"),
            value=desvio.get('causa_raiz') or '-',
            disabled=True,
            height=100,
            key=f"causa_readonly_{desvio_id}",
            label_visibility="collapsed"
        )

    with col_corr:
        st.markdown(f"**{t('Ação Corretiva')}**")
        st.text_area(
            t("Ação Corretiva"),
            value=desvio.get('acao_corretiva') or '-',
            disabled=True,
            height=100,
            key=f"corr_readonly_{desvio_id}",
            label_visibility="collapsed"
        )

    st.write("")

    # === SEÇÃO 10: Ação Preventiva ===
    st.markdown(f"**{t('Ação Preventiva')}**")
    st.text_area(
        t("Ação Preventiva"),
        value=desvio.get('acao_preventiva') or '-',
        disabled=True,
        height=100,
        key=f"prev_readonly_{desvio_id}",
        label_visibility="collapsed"
    )

    st.divider()

    # === SEÇÃO 11: Avaliações ===
    st.markdown(f"**{t('Avaliação do Investigador Principal')}**")
    st.text_area(
        t("Avaliação do Investigador Principal"),
        value=desvio.get('avaliacao_investigador') or '-',
        disabled=True,
        height=100,
        key=f"aval_inv_readonly_{desvio_id}",
        label_visibility="collapsed"
    )

    st.write("")

    # === SEÇÃO 12: Arquivamento e Submissão ===
    col20, col21, col22 = st.columns(3)

    with col20:
        with st.container(border=True):
            st.caption(t("Formulário Arquivado (ISF e TMF)?"))
            st.markdown(f"**{get_campo_traduzido(desvio, 'formulario_arquivado')}**")

    with col21:
        with st.container(border=True):
            st.caption(t("Data de Submissão ao CEP"))
            st.markdown(f"**{formatar_data(desvio.get('data_submissao_cep'))}**")

    with col22:
        with st.container(border=True):
            st.caption(t("Data de finalização"))
            st.markdown(f"**{formatar_data(desvio.get('data_finalizacao'))}**")

    st.write("")

    # === SEÇÃO 13: Observação ===
    st.markdown(f"**{t('Observação')}**")
    st.text_area(
        t("Observação"),
        value=desvio.get('observacao') or '-',
        disabled=True,
        height=100,
        key=f"obs_readonly_{desvio_id}",
        label_visibility="collapsed"
    )

    st.write("")

    # === SEÇÃO 14: Última Atualização ===
    with st.container(border=True):
        st.caption(t("Última Atualização"))
        st.markdown(f"**{formatar_data(desvio.get('data_atualizacao'))}**")


def lista_desvios_page():
    """Tela principal de listagem e avaliação de desvios"""
    estudo_codigo = st.session_state.get("estudo_codigo", "")
    estudo_nome = st.session_state.get("estudo_nome", "")
    estudo_id = st.session_state.get("estudo_id")

    # Cabeçalho
    st.title(f"📋 {estudo_codigo}")

    st.divider()

    # Barra de controles
    col_filtro, col_reload = st.columns([3, 1])

    with col_filtro:
        filtro_status = st.selectbox(
            t("Filtrar por status"),
            [t("Pendentes"), t("Todos"), t("Novo"), t("Modificado"), t("Avaliado")],
            index=0,
            label_visibility="collapsed",
        )

    with col_reload:
        if st.button(f"🔄 {t('Atualizar')}", use_container_width=True):
            limpar_cache()
            st.rerun()

    # Mapear filtro traduzido para valor do banco
    filtro_map = {
        t("Pendentes"): "Pendentes",
        t("Todos"): "Todos",
        t("Novo"): "Novo",
        t("Modificado"): "Modificado",
        t("Avaliado"): "Avaliado",
    }
    filtro_db = filtro_map.get(filtro_status, "Pendentes")

    # Carregar desvios com spinner
    with st.spinner(t("Carregando desvios...")):
        desvios = carregar_desvios_do_estudo(estudo_id, filtro_db)

    if not desvios:
        st.divider()
        if filtro_db == "Pendentes":
            st.success(t("Nenhum desvio pendente de avaliação!"))
        else:
            st.info(t("Nenhum desvio encontrado com o filtro selecionado."))
        return

    # Contador de resultados
    st.caption(f"{len(desvios)} {t('desvio(s) encontrado(s)')}")

    # Tabela de desvios
    df_display = pd.DataFrame(desvios)

    # Usar colunas traduzidas (_en) baseado no idioma
    lang = st.session_state.get("language", "pt")
    col_status = "status_en" if lang == "en" and "status_en" in df_display.columns else "status"
    col_importancia = "importancia_en" if lang == "en" and "importancia_en" in df_display.columns else "importancia"

    colunas_tabela = [
        "numero_desvio_estudo",
        col_status,
        "participante",
        "centro",
        "visita",
        col_importancia,
        "descricao_desvio",
    ]

    df_tabela = df_display[colunas_tabela].copy()
    df_tabela.columns = ["ID", t("Status"), t("Participante"), t("Centro"), t("Visita"), t("Importância"), t("Descrição")]

    df_tabela[t("Descrição")] = df_tabela[t("Descrição")].apply(
        lambda x: (x[:60] + "...") if x and len(x) > 60 else x
    )

    st.dataframe(
        df_tabela,
        use_container_width=True,
        hide_index=True,
    )

    st.markdown(" ")

    # Seção de avaliação (usando fragment para não re-renderizar a página toda)
    secao_avaliacao(desvios, estudo_id)


@st.fragment
def secao_avaliacao(desvios: list, estudo_id: int):
    """Fragment para seleção e avaliação de desvio - não re-renderiza a página toda"""

    st.subheader(t("Selecione um desvio para avaliar"))

    # Seletor de desvio
    opcoes_desvio = {f"{d['numero_desvio_estudo']}": d for d in desvios}

    desvio_selecionado_key = st.selectbox(
        t("Selecione o desvio:"),
        options=list(opcoes_desvio.keys()),
        index=None,
        placeholder=t("Selecione o ID do desvio..."),
        label_visibility="collapsed",
        key="select_desvio_fragment",
    )

    if not desvio_selecionado_key:
        st.info(t("Selecione um desvio na lista acima para visualizar os detalhes e realizar a avaliação."))
        return

    desvio = opcoes_desvio[desvio_selecionado_key]

    st.divider()

    # Container principal de avaliação
    with st.container(border=True):
        # Detalhes do desvio
        st.markdown(f"### ℹ️ {t('Detalhes do Desvio')}")
        exibir_detalhes_desvio(desvio)

        st.divider()

        # Seção de avaliação
        st.markdown(f"### {t('Sua Avaliação')}")

        avaliacao_atual = desvio.get("avaliacao_gerente_medico") or ""
        ja_avaliado = desvio["status"] == "Avaliado"

        nova_avaliacao = st.text_area(
            t("Avaliação do Gerente Médico"),
            value=avaliacao_atual,
            height=150,
            placeholder=t("Digite sua avaliação sobre este desvio..."),
            label_visibility="collapsed",
            disabled=ja_avaliado,
            key=f"avaliacao_text_{desvio['id']}",
        )

        if not ja_avaliado:
            st.write("")  # Espaçamento

            col_btn, _ = st.columns([1, 3])

            with col_btn:
                if st.button(t("Salvar Avaliação"), type="primary", use_container_width=True, key="btn_salvar_avaliacao"):
                    if not nova_avaliacao.strip():
                        st.warning(t("Por favor, preencha a avaliação antes de salvar."))
                    else:
                        with st.spinner(t("Salvando avaliação...")):
                            sucesso, mensagem = salvar_avaliacao(
                                desvio=desvio,
                                estudo_id=estudo_id,
                                avaliacao=nova_avaliacao.strip(),
                                row_version=desvio["row_version"],
                                valor_antigo=avaliacao_atual,
                                status_antigo=desvio["status"],
                            )

                        if sucesso:
                            st.success(t("Avaliação salva com sucesso!"))
                            st.rerun()
                        elif mensagem == "conflito":
                            st.error(t("Este desvio foi modificado por outra pessoa. Clique em 'Atualizar' para ver a versão mais recente."))
                        else:
                            st.error(f"{t('Erro ao salvar')}: {mensagem}")


# =========================
# Barra Lateral
# =========================

def render_sidebar():
    """Renderiza a barra lateral com informações do usuário e navegação"""
    with st.sidebar:
        # Seletor de idioma no topo com label traduzido
        idioma_opcoes = {"🌐 Português": "pt", "🌐 English": "en"}
        idioma_atual = st.session_state.get("language", "pt")
        opcao_atual = "🌐 Português" if idioma_atual == "pt" else "🌐 English"

        # Label traduzido acima do selectbox
        label_idioma = "Language" if idioma_atual == "en" else "Idioma"
        st.markdown(f"**{label_idioma}**")

        novo_idioma = st.selectbox(
            label_idioma,
            options=list(idioma_opcoes.keys()),
            index=list(idioma_opcoes.keys()).index(opcao_atual),
            label_visibility="collapsed",
            key="language_selector",
        )

        if idioma_opcoes[novo_idioma] != idioma_atual:
            st.session_state["language"] = idioma_opcoes[novo_idioma]
            st.rerun()

        st.markdown("---")

        st.markdown(f"### 👤 {t('Gerente Médico')}")
        st.write(st.session_state.get("gerente_nome", ""))
        st.caption(st.session_state.get("gerente_email", ""))

        if st.session_state.get("gerente_patrocinador"):
            st.markdown(f"**{t('Patrocinador')}:** {st.session_state['gerente_patrocinador']}")

        st.markdown("---")

        # Se já selecionou um estudo, mostrar opção de trocar
        if "estudo_id" in st.session_state:
            st.markdown(f"### 📚 {t('Estudo Atual')}")
            st.write(f"{st.session_state.get('estudo_codigo', '')}")
            st.caption(st.session_state.get("estudo_nome", ""))

            if st.button(f"🔀 {t('Trocar Estudo')}"):
                for k in ["estudo_id", "estudo_codigo", "estudo_nome"]:
                    st.session_state.pop(k, None)
                limpar_cache()
                st.rerun()

        st.markdown("---")

        if st.button(f"🚪 {t('Sair')}"):
            # Limpa cache e sessão (mantém idioma)
            limpar_cache()
            lang = st.session_state.get("language", "pt")
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state["language"] = lang
            st.rerun()


# =========================
# Main
# =========================

def main():
    st.set_page_config(
        page_title="Portal Gerente Médico",
        page_icon="🩺",
        layout="wide",
    )

    # Esconde elementos durante transição
    st.markdown("""
        <style>
        /* Remove animação de fade durante rerun */
        .stApp > header {
            transition: none !important;
        }
        .main .block-container {
            transition: none !important;
        }
        /* Esconde spinner padrão do Streamlit durante navegação */
        div[data-testid="stStatusWidget"] {
            visibility: hidden;
        }
        </style>
    """, unsafe_allow_html=True)

    # Inicializa flag de autenticação
    if "is_authenticated" not in st.session_state:
        st.session_state["is_authenticated"] = False

    # Fluxo de navegação
    if not st.session_state["is_authenticated"]:
        # Tela 1: Login
        login_screen()
        return

    # Renderiza sidebar após login
    render_sidebar()

    if "estudo_id" not in st.session_state:
        # Tela 2: Seleção de Estudo
        selecao_estudo_screen()
        return

    # Tela 3: Lista de Desvios e Avaliação
    lista_desvios_page()


if __name__ == "__main__":
    main()
