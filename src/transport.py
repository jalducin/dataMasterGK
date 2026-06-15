"""Transmisión de archivos a GK reutilizando una sola conexión por corrida.

Sustituye el patrón previo de abrir/cerrar una conexión SFTP/FTP por cada XML.
Soporta SFTP (paramiko) y FTP (ftplib), con reintento acotado por archivo y una
política de verificación de host key explícita y configurable.
"""
import os
import ftplib

import utils


class Transmisor:
    """Mantiene una conexión SFTP/FTP abierta para enviar varios XML.

    Uso:
        with Transmisor(cfg) as tx:
            for xml_path, name in archivos:
                ok = tx.enviar(xml_path, name)
    """

    def __init__(self, cfg=None, reintentos: int = 2, espera_seg: int = 2):
        cfg = cfg or utils.load_config()["server"][0]
        self.host = cfg["server"]
        self.user = cfg["user"]
        self.pwd = cfg["pwd"]
        self.remote_dir = cfg.get("pathUcon", "/")
        self.protocol = cfg.get("protocol", "ftp").lower()
        self.port = int(cfg.get("port", 22 if self.protocol == "sftp" else 21))
        # Política de host key: 'verificar' usa known_hosts; 'advertir' (por defecto)
        # registra una advertencia si no se verifica. Nunca acepta en silencio.
        self.host_key_policy = cfg.get("host_key_policy", "advertir").lower()
        self.reintentos = max(1, int(reintentos))
        self.espera_seg = espera_seg
        self._sftp = None
        self._transport = None
        self._ftp = None

    # -- ciclo de vida -------------------------------------------------------
    def __enter__(self):
        self.abrir()
        return self

    def __exit__(self, *exc):
        self.cerrar()
        return False

    def abrir(self):
        if self.protocol == "sftp":
            self._abrir_sftp()
        else:
            self._abrir_ftp()

    def _abrir_sftp(self):
        import paramiko
        utils.log_interfaces("INFO FTP", f"[SFTP] Conectando a {self.host}:{self.port}")
        self._transport = paramiko.Transport((self.host, self.port))
        self._transport.connect(username=self.user, password=self.pwd)
        self._aplicar_politica_host_key()
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        try:
            self._sftp.chdir(self.remote_dir)
        except IOError:
            self._sftp.mkdir(self.remote_dir)
            self._sftp.chdir(self.remote_dir)

    def _aplicar_politica_host_key(self):
        """Verifica la identidad del host según la política configurada."""
        if self.host_key_policy == "verificar":
            import paramiko
            clave_remota = self._transport.get_remote_server_key()
            conocidos = paramiko.HostKeys(os.path.expanduser("~/.ssh/known_hosts"))
            esperada = conocidos.lookup(self.host)
            if not esperada or esperada.get(clave_remota.get_name()) != clave_remota:
                raise paramiko.SSHException(
                    f"Host key de {self.host} no coincide con known_hosts (política 'verificar')."
                )
            utils.log_interfaces("INFO FTP", f"[SFTP] Host key de {self.host} verificada")
        else:
            utils.log_interfaces(
                "WARN FTP",
                f"[SFTP] Host key de {self.host} no verificada (política '{self.host_key_policy}')",
            )

    def _abrir_ftp(self):
        utils.log_interfaces("INFO FTP", f"[FTP] Conectando a {self.host}:{self.port}")
        self._ftp = ftplib.FTP()
        self._ftp.connect(self.host, self.port, timeout=30)
        self._ftp.login(self.user, self.pwd)
        self._ftp.set_pasv(True)
        self._ftp.cwd(self.remote_dir)

    def cerrar(self):
        for recurso, cerrar in (
            (self._sftp, lambda r: r.close()),
            (self._transport, lambda r: r.close()),
            (self._ftp, lambda r: r.quit()),
        ):
            if recurso is not None:
                try:
                    cerrar(recurso)
                except Exception:
                    pass
        self._sftp = self._transport = self._ftp = None

    # -- envío ---------------------------------------------------------------
    def enviar(self, xml_path: str, xml_name: str) -> bool:
        """Envía un XML reutilizando la conexión, con reintento acotado."""
        if not os.path.exists(xml_path):
            utils.log_interfaces("ERROR", f"No se encontró el XML a enviar: {xml_path}")
            return False

        ultimo_error = None
        for intento in range(1, self.reintentos + 1):
            try:
                if self.protocol == "sftp":
                    self._sftp.put(xml_path, xml_name)
                else:
                    with open(xml_path, "rb") as fh:
                        self._ftp.storbinary(f"STOR {xml_name}", fh)
                utils.log_interfaces(
                    "INFO FTP", f"{xml_name} enviado a {self.host}:{self.remote_dir} ({self.protocol})"
                )
                return True
            except Exception as e:
                ultimo_error = e
                utils.log_interfaces(
                    "ERROR FTP",
                    f"Intento {intento}/{self.reintentos} falló para {xml_name} → {self.host}: {e}",
                )
                if intento < self.reintentos:
                    self._reconectar()

        utils._registrar_error_ftp(xml_name, ultimo_error)
        return False

    def _reconectar(self):
        import time
        time.sleep(self.espera_seg)
        try:
            self.cerrar()
        except Exception:
            pass
        try:
            self.abrir()
        except Exception as e:
            utils.log_interfaces("ERROR FTP", f"No se pudo reconectar a {self.host}: {e}")
