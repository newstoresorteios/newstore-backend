// src/AutoPaySection.jsx
import * as React from "react";
import {
  Paper,
  Stack,
  Typography,
  Switch,
  Chip,
  Button,
  TextField,
  Divider,
  Box,
  Tooltip,
  CircularProgress,
} from "@mui/material";
import CreditCardIcon from "@mui/icons-material/CreditCard";
import AutorenewRoundedIcon from "@mui/icons-material/AutorenewRounded";
import ClearRoundedIcon from "@mui/icons-material/ClearRounded";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { authHeaders as _authHeaders } from "./lib/api"; // se não tiver, usamos o fallback abaixo

/* ============================================================
 * Helpers de API (fallback caso não exista ./lib/api)
 * ============================================================ */
const apiJoin = (p) => {
  const base =
    process.env.REACT_APP_API_BASE_URL ||
    process.env.REACT_APP_API_BASE ||
    "/api";
  return `${String(base).replace(/\/+$/, "")}${p.startsWith("/") ? p : `/${p}`}`;
};

const defaultAuthHeaders = () => {
  const tk =
    localStorage.getItem("ns_auth_token") ||
    sessionStorage.getItem("ns_auth_token") ||
    localStorage.getItem("token") ||
    localStorage.getItem("access_token") ||
    sessionStorage.getItem("token");
  return tk
    ? { Authorization: `Bearer ${String(tk).replace(/^Bearer\s+/i, "")}` }
    : {};
};

const authHeaders = _authHeaders || defaultAuthHeaders;

/* ============================================================
 * Utils locais
 * ============================================================ */
const pad2 = (n) => String(n).padStart(2, "0");
const onlyDigits = (s) => String(s || "").replace(/\D+/g, "");
const guessDocType = (docDigits) =>
  (String(docDigits || "").length > 11 ? "CNPJ" : "CPF");

// aceita "MM/AA", "MM/AAAA", "MMAA", "MMYYYY"
function parseExpiry(exp) {
  const d = onlyDigits(exp);
  if (d.length < 4) return { mm: "", yyyy: "" };
  const mm = d.slice(0, 2);
  let yy = d.slice(2);
  let yyyy = yy.length === 2 ? `20${yy}` : yy.slice(0, 4);
  return { mm, yyyy };
}

/** Carrega o SDK v2 do Mercado Pago se ainda não estiver presente. */
async function loadMpSdkOnce() {
  if (window.MercadoPago) return true;
  if (window.__mpSdkPromise) return window.__mpSdkPromise;

  window.__mpSdkPromise = new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://sdk.mercadopago.com/js/v2";
    s.async = true;
    s.onload = () => resolve(true);
    s.onerror = () =>
      reject(new Error("Falha ao carregar o SDK do Mercado Pago."));
    document.head.appendChild(s);
  });

  try {
    await window.__mpSdkPromise;
    return true;
  } catch (e) {
    window.__mpSdkPromise = null;
    throw e;
  }
}

/* ============================================================
 * Componente
 * ============================================================ */
export default function AutoPaySection() {
  const [loading, setLoading] = React.useState(true);
  const [saving, setSaving] = React.useState(false);

  const [active, setActive] = React.useState(true);
  const [savedActive, setSavedActive] = React.useState(true);

  const [numbers, setNumbers] = React.useState([]); // seleção atual
  const [savedNumbers, setSavedNumbers] = React.useState([]); // última salva

  const [card, setCard] = React.useState({
    brand: null,
    last4: null,
    has_card: false,
  });

  const [holder, setHolder] = React.useState("");
  const [savedHolder, setSavedHolder] = React.useState("");

  the [doc, setDoc] = React.useState("");
  const [savedDoc, setSavedDoc] = React.useState("");

  // Campos para gerar token (SDK)
  const [cardNumber, setCardNumber] = React.useState("");
  const [expiry, setExpiry] = React.useState(""); // MM/AA
  const [cvv, setCvv] = React.useState("");

  // Mensagem informativa quando não houver número
  const needsAtLeastOne = numbers.length === 0;

  // --- Dirty flags (reabilitar botão quando houver mudança) ---
  const numbersDirty = React.useMemo(() => {
    if (!Array.isArray(numbers) || !Array.isArray(savedNumbers)) return false;
    if (numbers.length !== savedNumbers.length) return true;
    const a = [...numbers].sort((x, y) => x - y).join(",");
    const b = [...savedNumbers].sort((x, y) => x - y).join(",");
    return a !== b;
  }, [numbers, savedNumbers]);

  const activeDirty = active !== savedActive;
  const holderDirty = (holder || "") !== (savedHolder || "");
  const docDirty = (doc || "") !== (savedDoc || "");
  const cardFieldsDirty = !!(cardNumber || expiry || cvv); // só de preencher algo já tentaremos tokenizar

  // Exige pelo menos 1 número e alguma alteração
  const anyDirty =
    numbersDirty || activeDirty || holderDirty || docDirty || cardFieldsDirty;

  const canSave = !loading && !saving && !needsAtLeastOne && anyDirty;

  // Carrega perfil
  React.useEffect(() => {
    let alive = true;
    (async () => {
      try {
        setLoading(true);
        const r = await fetch(apiJoin("/api/me/autopay"), {
          headers: { "Content-Type": "application/json", ...authHeaders() },
          credentials: "include",
        });
        const j = await r.json().catch(() => null);
        if (alive && j) {
          const gotNumbers = Array.isArray(j.numbers)
            ? j.numbers.map(Number)
            : [];
          setActive(!!j.active);
          setSavedActive(!!j.active);

          setNumbers(gotNumbers);
          setSavedNumbers(gotNumbers);

          setCard({
            brand: j.brand || null,
            last4: j.last4 || null,
            has_card: !!(j.brand || j.last4),
          });

          setHolder(j.holder_name || "");
          setSavedHolder(j.holder_name || "");

          setDoc(j.doc_number || "");
          setSavedDoc(j.doc_number || "");
        }
      } catch (e) {
        console.error("[autopay] GET error:", e?.message || e);
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  function toggle(n) {
    setNumbers((prev) => {
      const next = prev.includes(n)
        ? prev.filter((x) => x !== n)
        : [...prev, n].slice(0, 20); // segurança de 20
      return next;
    });
  }

  // Gera token via SDK do Mercado Pago (v2)
  async function createMpTokenOrFail() {
    const num = onlyDigits(cardNumber);
    const { mm, yyyy } = parseExpiry(expiry);
    const sc = onlyDigits(cvv).slice(0, 4);
    const holderName = (holder || "").trim();
    const docDigits = onlyDigits(doc);

    if (!num || !mm || !yyyy || !sc || !holderName || !docDigits) {
      throw new Error("Dados do cartão incompletos.");
    }

    await loadMpSdkOnce();

    const PK =
      process.env.REACT_APP_MP_PUBLIC_KEY ||
      process.env.REACT_APP_MERCADOPAGO_PUBLIC_KEY ||
      window.MP_PUBLIC_KEY;

    if (!PK) {
      throw new Error(
        "Chave pública do Mercado Pago não configurada (REACT_APP_MP_PUBLIC_KEY)."
      );
    }
    if (!window.MercadoPago) {
      throw new Error("SDK do Mercado Pago não pôde ser carregado.");
    }

    const mp = new window.MercadoPago(PK, { locale: "pt-BR" });

    // --- v2: método correto é createCardToken (não mp.cardToken.create) ---
    if (typeof mp.createCardToken !== "function") {
      throw new Error("Função de tokenização indisponível.");
    }

    const payload = {
      cardNumber: num,
      securityCode: sc,
      cardExpirationMonth: mm,   // nomes que o v2 espera
      cardExpirationYear: yyyy,
      cardholderName: holderName,
      identificationType: guessDocType(docDigits),
      identificationNumber: docDigits,
    };

    const resp = await mp.createCardToken(payload);
    const tok =
      resp?.id || resp?.data?.id || resp?.token || resp?.cardTokenId;
    if (!tok) throw new Error("Falha ao gerar token do cartão.");
    return tok;
  }

  async function save() {
    if (needsAtLeastOne) {
      alert("Selecione pelo menos 1 número para salvar as preferências.");
      return;
    }

    setSaving(true);
    try {
      const body = {
        active,
        numbers,
        holder_name: holder,
        doc_number: doc,
      };

      // regra: se mexeu em qualquer campo de cartão (ou atualizou titular/doc), tokeniza
      const wantsCardUpdate = cardFieldsDirty;

      if (wantsCardUpdate) {
        const token = await createMpTokenOrFail();
        body.card_token = token;
      }

      const r = await fetch(apiJoin("/api/me/autopay"), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        credentials: "include",
        body: JSON.stringify(body),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j?.error || "save_failed");

      // Atualiza estados "salvos" para zerar dirty e re-desabilitar botão até nova mudança
      setSavedNumbers([...numbers]);
      setSavedActive(active);
      setSavedHolder(holder);
      setSavedDoc(doc);

      setCard(j.card || { has_card: false, brand: null, last4: null });
      if (j.card?.has_card) {
        // limpa campos sensíveis
        setCardNumber("");
        setExpiry("");
        setCvv("");
      }

      alert("Preferências salvas!");
    } catch (e) {
      console.error("[autopay] save error:", e?.message || e);
      alert(
        e?.message === "save_failed"
          ? "Falha ao salvar. Verifique os dados do cartão e tente novamente."
          : e?.message || "Falha ao salvar preferências."
      );
    } finally {
      setSaving(false);
    }
  }

  async function cancelAutopay() {
    if (
      !window.confirm(
        "Tem certeza que deseja cancelar a compra automática? Isso apagará os números cativos e o cartão salvo."
      )
    ) {
      return;
    }
    setSaving(true);
    try {
      const r = await fetch(apiJoin("/api/me/autopay/cancel"), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        credentials: "include",
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j?.error || "cancel_failed");

      // zera tudo
      setActive(false);
      setSavedActive(false);

      setNumbers([]);
      setSavedNumbers([]);

      setCard({ brand: null, last4: null, has_card: false });

      setCardNumber("");
      setExpiry("");
      setCvv("");

      alert("Compra automática cancelada.");
    } catch (e) {
      console.error("[autopay] cancel error:", e?.message || e);
      alert("Não foi possível cancelar agora.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Paper variant="outlined" sx={{ p: { xs: 2, md: 3 } }}>
      <Stack spacing={2}>
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="space-between"
        >
          <Typography variant="h6" fontWeight={900}>
            Compra automática (cartão)
          </Typography>
          <Stack direction="row" spacing={1} alignItems="center">
            <Typography variant="body2" sx={{ opacity: 0.8 }}>
              Ativar
            </Typography>
            <Switch
              checked={active}
              onChange={(e) => setActive(e.target.checked)}
            />
          </Stack>
        </Stack>

        <Typography variant="body2" sx={{ opacity: 0.8, mt: -1 }}>
          Cadastre seu cartão e escolha números “cativos”. Quando um novo sorteio
          abrir, cobraremos automaticamente e reservaremos seus números (pagamento
          via Mercado Pago).
          <br />
          <span style={{ opacity: 0.75 }}>
            O CVV e a validade são exigidos apenas para salvar/atualizar o cartão.
          </span>
        </Typography>

        {/* Cartão salvo */}
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            gap: 1,
            p: 1.25,
            border: "1px solid rgba(255,255,255,.08)",
            borderRadius: 2.5,
            bgcolor: "rgba(255,255,255,.03)",
          }}
        >
          <CreditCardIcon sx={{ opacity: 0.9 }} />
          <Typography sx={{ fontWeight: 800 }}>
            {card.has_card
              ? `${card.brand || "Cartão"} •••• ${card.last4}`
              : "Nenhum cartão salvo"}
          </Typography>
        </Box>

        {/* Atualizar/Salvar cartão */}
        <Stack spacing={1}>
          <Typography variant="subtitle2" sx={{ opacity: 0.85 }}>
            Atualizar cartão (opcional)
          </Typography>

          <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
            <TextField
              label="Número do cartão"
              inputMode="numeric"
              value={cardNumber}
              onChange={(e) =>
                setCardNumber(onlyDigits(e.target.value).slice(0, 19))
              }
              fullWidth
            />
            <TextField
              label="Nome impresso no cartão"
              value={holder}
              onChange={(e) => setHolder(e.target.value)}
              fullWidth
            />
          </Stack>

          <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
            <TextField
              label="CPF/CNPJ do titular"
              value={doc}
              onChange={(e) => setDoc(onlyDigits(e.target.value).slice(0, 18))}
              fullWidth
            />
            <TextField
              label="Validade (MM/AA)"
              placeholder="ex.: 04/27"
              value={expiry}
              onChange={(e) => setExpiry(e.target.value)}
              sx={{ maxWidth: 180 }}
            />
            <TextField
              label="CVV"
              inputMode="numeric"
              value={cvv}
              onChange={(e) => setCvv(onlyDigits(e.target.value).slice(0, 4))}
              sx={{ maxWidth: 140 }}
            />
          </Stack>
        </Stack>

        <Divider />

        {/* Escolha dos números cativos */}
        <Typography variant="subtitle2" sx={{ opacity: 0.85 }}>
          Números cativos (clique para selecionar)
        </Typography>

        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: {
              xs: "repeat(6, 1fr)",
              sm: "repeat(10, 1fr)",
              md: "repeat(12, 1fr)",
            },
            gap: 0.6,
          }}
        >
          {Array.from({ length: 100 }, (_, i) => i).map((n) => {
            const on = numbers.includes(n);
            return (
              <Tooltip key={n} title={on ? "Remover" : "Adicionar"} arrow>
                <Chip
                  label={pad2(n)}
                  onClick={() => toggle(n)}
                  clickable
                  sx={{
                    fontWeight: 800,
                    borderRadius: 999,
                    border: on
                      ? "1px solid #9BD1FF"
                      : "1px solid rgba(255,255,255,.14)",
                    bgcolor: on
                      ? "rgba(155,209,255,.15)"
                      : "rgba(255,255,255,.04)",
                    color: on ? "#D6EBFF" : "inherit",
                    "&:hover": {
                      bgcolor: on
                        ? "rgba(155,209,255,.25)"
                        : "rgba(255,255,255,.08)",
                    },
                  }}
                />
              </Tooltip>
            );
          })}
        </Box>

        {/* Ações */}
        <Stack
          direction="row"
          spacing={1}
          justifyContent="space-between"
          alignItems="center"
        >
          <Stack
            direction="row"
            spacing={1}
            alignItems="center"
            sx={{ opacity: needsAtLeastOne ? 0.95 : 0.6 }}
          >
            <InfoOutlinedIcon fontSize="small" />
            <Typography variant="body2">
              Selecione <b>pelo menos 1 número</b> para salvar.
            </Typography>
          </Stack>

          <Stack direction="row" spacing={1}>
            <Button
              variant="outlined"
              color="error"
              startIcon={<ClearRoundedIcon />}
              onClick={cancelAutopay}
              disabled={saving || loading}
            >
              Cancelar compra automática
            </Button>

            <Button
              variant="contained"
              startIcon={
                saving ? <CircularProgress size={16} /> : <AutorenewRoundedIcon />
              }
              onClick={save}
              disabled={!canSave}
            >
              {saving ? "Salvando…" : "Salvar preferências"}
            </Button>
          </Stack>
        </Stack>
      </Stack>
    </Paper>
  );
}
