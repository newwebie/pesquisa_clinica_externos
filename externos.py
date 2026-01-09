"""
Sistema de Avaliação do Gerente Médico
Portal externo para avaliação de desvios de estudos clínicos
"""

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone


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
# Autenticação do Gerente Médico
# =========================

def login_screen():
    """Tela de login - verifica email na tabela gerentes_medicos"""
    st.title("🔐 Portal do Gerente Médico")
    st.write("Informe seu e-mail cadastrado para acessar o sistema de avaliação de desvios.")

    email = st.text_input("E-mail", placeholder="seu.email@empresa.com")

    if st.button("Entrar", type="primary"):
        if not email:
            st.warning("Por favor, informe um e-mail.")
            return

        with st.spinner("Verificando credenciais..."):
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
                    st.error("E-mail não cadastrado como Gerente Médico. Entre em contato com o administrador.")
                    return

                # Guarda informações na sessão
                st.session_state["is_authenticated"] = True
                st.session_state["gerente_id"] = gerente["id"]
                st.session_state["gerente_nome"] = gerente["nome"]
                st.session_state["gerente_email"] = gerente["email"]
                st.session_state["gerente_patrocinador"] = gerente["patrocinador"]

                st.success(f"Bem-vindo(a), {gerente['nome']}!")
                st.rerun()

            except Exception as e:
                st.error(f"Erro ao autenticar: {e}")
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
            LEFT JOIN desvios d ON d.estudo_id = e.id
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
            LEFT JOIN desvios d ON d.estudo_id = e.id
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
    st.title("📚 Meus Estudos")
    st.caption("Selecione um estudo para avaliar os desvios")

    email = st.session_state["gerente_email"]

    # Carregar dados (com cache e spinner)
    with st.spinner("Carregando estudos..."):
        estudos = carregar_estudos_do_gerente(email)
        metricas = carregar_metricas_gerente(email)

    if not estudos:
        st.warning("Você não está alocado em nenhum estudo ativo.")
        st.info("Entre em contato com o Gerente de Projetos para ser alocado a um estudo.")
        return

    # Painel de métricas no topo com containers
    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.metric(
                label="Estudos",
                value=metricas["total_estudos"],
            )

    with col2:
        with st.container(border=True):
            st.metric(
                label="Desvios Pendentes",
                value=metricas["total_pendentes"],
            )

    with col3:
        with st.container(border=True):
            st.metric(
                label="Estudos com Pendência",
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
                        st.write(f"🔴 {pendentes} pendência(s)")
                    else:
                        st.write("🟢 Sem pendências")

                    if st.button(
                        "Acessar",
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

        # Query base
        query = """
            SELECT
                id,
                numero_desvio_estudo,
                status,
                participante,
                centro,
                visita,
                descricao_desvio,
                identificacao_desvio,
                causa_raiz,
                acao_preventiva,
                acao_corretiva,
                importancia,
                avaliacao_gerente_medico,
                data_atualizacao,
                xmin AS row_version
            FROM desvios
            WHERE estudo_id = %s
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


def salvar_avaliacao(desvio_id, estudo_id, avaliacao, row_version, valor_antigo, status_antigo):
    """
    Salva a avaliação do gerente médico e atualiza o status para 'Avaliado'.
    Registra as alterações no log.
    """
    gerente_nome = st.session_state["gerente_nome"]

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

        return True, "sucesso"

    except Exception as e:
        return False, str(e)
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def exibir_detalhes_desvio(desvio):
    """Exibe os detalhes do desvio em formato somente leitura"""
    desvio_id = desvio['id']  # Para keys únicas

    # Linha 1: ID, Status, Importância
    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.caption("ID do Desvio")
            st.markdown(f"**#{desvio['numero_desvio_estudo']}**")

    with col2:
        with st.container(border=True):
            st.caption("Status")
            status = desvio['status']
            if status == 'Avaliado':
                st.success(status)
            else:
                st.markdown(f"**{status}**")

    with col3:
        with st.container(border=True):
            st.caption("Importância")
            importancia = desvio['importancia'] or '-'
            if importancia == 'Maior':
                st.error(importancia)
            elif importancia == 'Menor':
                st.markdown(f"**{importancia}**")
            else:
                st.markdown(f"**{importancia}**")

    st.write("")

    # Linha 2: Participante, Centro, Visita
    col4, col5, col6 = st.columns(3)

    with col4:
        with st.container(border=True):
            st.caption("Participante")
            st.markdown(f"**{desvio['participante'] or '-'}**")

    with col5:
        with st.container(border=True):
            st.caption("Centro")
            st.markdown(f"**{desvio['centro'] or '-'}**")

    with col6:
        with st.container(border=True):
            st.caption("Visita")
            st.markdown(f"**{desvio['visita'] or '-'}**")

    st.write("")

    # Linha 3: Identificação e Data
    col7, col8 = st.columns(2)

    with col7:
        with st.container(border=True):
            st.caption("Identificação do Desvio")
            st.markdown(f"**{desvio['identificacao_desvio'] or '-'}**")

    with col8:
        with st.container(border=True):
            st.caption("Última Atualização")
            if desvio['data_atualizacao']:
                data_raw = desvio['data_atualizacao']
                if hasattr(data_raw, 'strftime'):
                    # Converte para UTC-3 (Brasília)
                    utc_minus_3 = timezone(timedelta(hours=-3))
                    if data_raw.tzinfo is None:
                        # Se não tem timezone, assume UTC e converte
                        data_utc = data_raw.replace(tzinfo=timezone.utc)
                        data_brasilia = data_utc.astimezone(utc_minus_3)
                    else:
                        data_brasilia = data_raw.astimezone(utc_minus_3)
                    data_formatada = data_brasilia.strftime("%d/%m/%Y %H:%M")
                else:
                    data_formatada = str(data_raw)
                st.markdown(f"**{data_formatada}**")
            else:
                st.markdown("**-**")

    st.divider()

    # Linha 1: Descrição e Ação Corretiva
    col_desc, col_corr = st.columns(2)

    with col_desc:
        st.markdown("**Descrição do Desvio**")
        st.text_area(
            "Descrição",
            value=desvio['descricao_desvio'] or '-',
            disabled=True,
            height=100,
            key=f"desc_readonly_{desvio_id}",
            label_visibility="collapsed"
        )

    with col_corr:
        st.markdown("**Ação Corretiva**")
        st.text_area(
            "Ação Corretiva",
            value=desvio['acao_corretiva'] or '-',
            disabled=True,
            height=100,
            key=f"corr_readonly_{desvio_id}",
            label_visibility="collapsed"
        )

    st.write("")

    # Linha 2: Causa Raiz e Ação Preventiva
    col_causa, col_prev = st.columns(2)

    with col_causa:
        st.markdown("**Causa Raiz**")
        st.text_area(
            "Causa Raiz",
            value=desvio['causa_raiz'] or '-',
            disabled=True,
            height=100,
            key=f"causa_readonly_{desvio_id}",
            label_visibility="collapsed"
        )

    with col_prev:
        st.markdown("**Ação Preventiva**")
        st.text_area(
            "Ação Preventiva",
            value=desvio['acao_preventiva'] or '-',
            disabled=True,
            height=100,
            key=f"prev_readonly_{desvio_id}",
            label_visibility="collapsed"
        )


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
            "Filtrar por status",
            ["Pendentes", "Todos", "Novo", "Modificado", "Avaliado"],
            index=0,
            label_visibility="collapsed",
        )

    with col_reload:
        if st.button("🔄 Atualizar", use_container_width=True):
            limpar_cache()
            st.rerun()

    # Carregar desvios com spinner
    with st.spinner("Carregando desvios..."):
        desvios = carregar_desvios_do_estudo(estudo_id, filtro_status)

    if not desvios:
        st.divider()
        if filtro_status == "Pendentes":
            st.success("Nenhum desvio pendente de avaliação!")
        else:
            st.info("Nenhum desvio encontrado com o filtro selecionado.")
        return

    # Contador de resultados
    st.caption(f"{len(desvios)} desvio(s) encontrado(s)")

    # Tabela de desvios
    df_display = pd.DataFrame(desvios)

    colunas_tabela = [
        "numero_desvio_estudo",
        "status",
        "participante",
        "centro",
        "visita",
        "importancia",
        "descricao_desvio",
    ]

    df_tabela = df_display[colunas_tabela].copy()
    df_tabela.columns = ["ID", "Status", "Participante", "Centro", "Visita", "Importância", "Descrição"]

    df_tabela["Descrição"] = df_tabela["Descrição"].apply(
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

    st.subheader("Selecione um desvio para avaliar")

    # Seletor de desvio
    opcoes_desvio = {f"{d['numero_desvio_estudo']}": d for d in desvios}

    desvio_selecionado_key = st.selectbox(
        "Selecione o desvio:",
        options=list(opcoes_desvio.keys()),
        index=None,
        placeholder="Selecione o ID do desvio...",
        label_visibility="collapsed",
        key="select_desvio_fragment",
    )

    if not desvio_selecionado_key:
        st.info("Selecione um desvio na lista acima para visualizar os detalhes e realizar a avaliação.")
        return

    desvio = opcoes_desvio[desvio_selecionado_key]

    st.divider()

    # Container principal de avaliação
    with st.container(border=True):
        # Detalhes do desvio
        st.markdown("### ℹ️ Detalhes do Desvio")
        exibir_detalhes_desvio(desvio)

        st.divider()

        # Seção de avaliação
        st.markdown("### Sua Avaliação")

        avaliacao_atual = desvio.get("avaliacao_gerente_medico") or ""
        ja_avaliado = desvio["status"] == "Avaliado"

        nova_avaliacao = st.text_area(
            "Avaliação do Gerente Médico",
            value=avaliacao_atual,
            height=150,
            placeholder="Digite sua avaliação sobre este desvio...",
            label_visibility="collapsed",
            disabled=ja_avaliado,
            key=f"avaliacao_text_{desvio['id']}",
        )

        if not ja_avaliado:
            st.write("")  # Espaçamento

            col_btn, _ = st.columns([1, 3])

            with col_btn:
                if st.button("Salvar Avaliação", type="primary", use_container_width=True, key="btn_salvar_avaliacao"):
                    if not nova_avaliacao.strip():
                        st.warning("Por favor, preencha a avaliação antes de salvar.")
                    else:
                        with st.spinner("Salvando avaliação..."):
                            sucesso, mensagem = salvar_avaliacao(
                                desvio_id=desvio["id"],
                                estudo_id=estudo_id,
                                avaliacao=nova_avaliacao.strip(),
                                row_version=desvio["row_version"],
                                valor_antigo=avaliacao_atual,
                                status_antigo=desvio["status"],
                            )

                        if sucesso:
                            st.success("Avaliação salva com sucesso!")
                            st.rerun()
                        elif mensagem == "conflito":
                            st.error(
                                "Este desvio foi modificado por outra pessoa. "
                                "Clique em 'Atualizar' para ver a versão mais recente."
                            )
                        else:
                            st.error(f"Erro ao salvar: {mensagem}")


# =========================
# Barra Lateral
# =========================

def render_sidebar():
    """Renderiza a barra lateral com informações do usuário e navegação"""
    with st.sidebar:
        st.markdown("### 👤 Gerente Médico")
        st.write(st.session_state.get("gerente_nome", ""))
        st.caption(st.session_state.get("gerente_email", ""))

        if st.session_state.get("gerente_patrocinador"):
            st.markdown(f"**Patrocinador:** {st.session_state['gerente_patrocinador']}")

        st.markdown("---")

        # Se já selecionou um estudo, mostrar opção de trocar
        if "estudo_id" in st.session_state:
            st.markdown("### 📚 Estudo Atual")
            st.write(f"{st.session_state.get('estudo_codigo', '')}")
            st.caption(st.session_state.get("estudo_nome", ""))

            if st.button("🔀 Trocar Estudo"):
                for k in ["estudo_id", "estudo_codigo", "estudo_nome"]:
                    st.session_state.pop(k, None)
                limpar_cache()
                st.rerun()

        st.markdown("---")

        if st.button("🚪 Sair"):
            # Limpa cache e sessão
            limpar_cache()
            for key in list(st.session_state.keys()):
                del st.session_state[key]
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
