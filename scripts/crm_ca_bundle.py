"""Genera el CA bundle para hablar con el CRM sin apagar la verificación TLS.

POR QUÉ HACE FALTA
------------------
El CRM presenta un certificado válido de Sectigo (`*.suizoargentina.com`) pero **no
envía la cadena intermedia**. Los navegadores no lo notan porque descargan el
intermedio solos (AIA fetching); Python/requests no lo hace y falla con:

    SSLError: certificate verify failed: unable to get local issuer certificate

La solución de fondo es que infra agregue el intermedio a la config del servidor.
Mientras tanto, este script arma un bundle = CAs de certifi + el intermedio que el
servidor omite, y SIEM lo usa vía CRM_CA_BUNDLE. La verificación sigue ACTIVA: se
valida la cadena completa, solo que aportando nosotros el eslabón que falta.

Uso:
    python -m scripts.crm_ca_bundle                     # usa CRM_BASE_URL del .env
    python -m scripts.crm_ca_bundle --host otro.host    # contra otro host
    python -m scripts.crm_ca_bundle --salida ruta.pem

Después, en web_comparativas/.env:
    CRM_CA_BUNDLE=<ruta que imprime este script>

Si el intermedio rota (o cambia el certificado), volver a correrlo.
"""
from __future__ import annotations

import argparse
import os
import socket
import ssl
import sys
from urllib.parse import urlparse

import certifi
import requests
from cryptography import x509
from cryptography.hazmat.backends import default_backend

DEFAULT_SALIDA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "web_comparativas", "crm_ca_bundle.pem",
)


def _host_desde_env() -> str | None:
    base = (os.getenv("CRM_BASE_URL") or "").strip()
    if not base:
        return None
    return urlparse(base).hostname


def _cert_del_servidor(host: str, puerto: int = 443) -> bytes:
    """Baja el certificado hoja SIN verificar (solo para leer su AIA)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, puerto), timeout=20) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as tls:
            return tls.getpeercert(binary_form=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Arma el CA bundle del CRM.")
    ap.add_argument("--host", help="Host del CRM (default: el de CRM_BASE_URL).")
    ap.add_argument("--salida", default=DEFAULT_SALIDA, help="Archivo .pem a escribir.")
    args = ap.parse_args()

    host = args.host or _host_desde_env()
    if not host:
        print("ERROR: no hay host. Pasá --host o definí CRM_BASE_URL en el .env.")
        return 2

    print(f"Host: {host}")
    hoja = x509.load_der_x509_certificate(_cert_del_servidor(host), default_backend())
    print(f"  certificado: {hoja.subject.rfc4514_string()}")
    print(f"  emisor     : {hoja.issuer.rfc4514_string()}")

    try:
        aia = hoja.extensions.get_extension_for_class(x509.AuthorityInformationAccess).value
    except x509.ExtensionNotFound:
        print("ERROR: el certificado no declara AIA; pedí el intermedio a infra.")
        return 1
    urls = [
        d.access_location.value for d in aia
        if d.access_method == x509.oid.AuthorityInformationAccessOID.CA_ISSUERS
    ]
    if not urls:
        print("ERROR: el AIA no trae CA Issuers; pedí el intermedio a infra.")
        return 1

    print(f"  intermedio : {urls[0]}")
    der = requests.get(urls[0], timeout=30).content
    inter = x509.load_der_x509_certificate(der, default_backend())
    print(f"               {inter.subject.rfc4514_string()}")

    with open(args.salida, "wb") as f:
        f.write(open(certifi.where(), "rb").read())
        f.write(b"\n" + ssl.DER_cert_to_PEM_cert(der).encode())
    print(f"\nBundle escrito: {args.salida}")

    # Prueba real: si esto no verifica, el bundle no sirve y hay que saberlo ACÁ.
    try:
        resp = requests.get(f"https://{host}/Api/access_token", timeout=30, verify=args.salida)
        print(f"Verificación TLS con el bundle: OK (HTTP {resp.status_code})")
    except requests.exceptions.SSLError as exc:
        print(f"Verificación TLS con el bundle: FALLA -> {str(exc)[:200]}")
        return 1

    print(f"\nAgregá esto a web_comparativas/.env:\n    CRM_CA_BUNDLE={args.salida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
