package samples.util;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class MyDoc implements Serializable {

    private static final long serialVersionUID = 6880667215923483985L;

    private long id;
    private String status;
}
