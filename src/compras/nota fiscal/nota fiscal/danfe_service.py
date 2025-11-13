# danfe_service.py
# Lê XML puro via stdin e devolve PDF (base64) na stdout.

import sys
import base64

def read_stdin_text() -> str:
    data = sys.stdin.read()
    if not data:
        print("No XML on stdin", file=sys.stderr)
        sys.exit(2)
    return data

def try_brazilfiscalreport(xml_text: str) -> bytes | None:
    try:
        from brazilfiscalreport.danfe import Danfe
    except Exception as e:
        print(f"[brazilfiscalreport import] {e}", file=sys.stderr)
        return None
    try:
        # A lib espera STRING; usa .output(caminho) ou retorna bytes se disponível
        d = Danfe(xml_text)
        if hasattr(d, "output"):
            import tempfile, os
            fd, tmp = tempfile.mkstemp(suffix=".pdf")
            os.close(fd)
            try:
                d.output(tmp)
                with open(tmp, "rb") as f:
                    return f.read()
            finally:
                try: os.remove(tmp)
                except: pass
        if hasattr(d, "to_pdf"):
            return d.to_pdf()
        if hasattr(d, "render_pdf"):
            return d.render_pdf()
        return None
    except Exception as e:
        print(f"[brazilfiscalreport exec] {e}", file=sys.stderr)
        return None

def try_erpbrasil(xml_text: str) -> bytes | None:
    try:
        from nfelib.nfe.bindings.v4_0.proc_nfe_v4_00 import NfeProc
        from erpbrasil.edoc.pdf import danfe as _engine  # força engine
    except Exception as e:
        print(f"[erpbrasil import] {e}", file=sys.stderr)
        return None
    try:
        nfe_proc = NfeProc.from_string(xml_text)
        if hasattr(nfe_proc, "to_pdf"):
            return nfe_proc.to_pdf(engine="erpbrasil.edoc.pdf")
        return None
    except Exception as e:
        print(f"[erpbrasil nfeProc] {e}", file=sys.stderr)
        # tenta quando vier só <NFe>
        try:
            from nfelib.nfe.bindings.v4_0.nfe_v4_00 import NFe
            nfe = NFe.from_string(xml_text)
            if hasattr(nfe, "to_pdf"):
                return nfe.to_pdf(engine="erpbrasil.edoc.pdf")
        except Exception as e2:
            print(f"[erpbrasil NFe] {e2}", file=sys.stderr)
        return None

def main():
    xml_text = read_stdin_text()

    # Tenta BrazilFiscalReport
    pdf = try_brazilfiscalreport(xml_text)
    if pdf is None:
        # Fallback: erpbrasil
        pdf = try_erpbrasil(xml_text)

    if not pdf:
        print("Failed to generate DANFE", file=sys.stderr)
        sys.exit(1)

    # devolve base64 do PDF na stdout
    sys.stdout.write(base64.b64encode(pdf).decode("ascii"))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
