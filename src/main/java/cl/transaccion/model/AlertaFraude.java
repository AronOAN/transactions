package cl.transaccion.model;

import cl.transaccion.model.Sev;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlertaFraude {


 private String usuario;
 private Double monto;
 private String motivo;
 private Sev severidad; // "ALTA", "MEDIA"

}

