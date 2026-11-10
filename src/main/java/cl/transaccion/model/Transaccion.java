package cl.transaccion.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import cl.transaccion.model.Origen;

@Data
@AllArgsConstructor
@NoArgsConstructor


public class Transaccion {

    
    private String usuario;
    private Double monto;
    private Origen origen; // Enum con "cajero", "web", "app"(para agregar mas si hay mas adelante)
    private Long timestamp; // Epoch time (milisegundos)



}