from multiprocessing.managers import Server


class RemoteServer(Server):
  
  def accept_connection(self, c: Connection, name: str) -> None:
    return super().accept_connection(c, name)