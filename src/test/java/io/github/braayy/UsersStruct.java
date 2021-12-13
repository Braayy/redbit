package io.github.braayy;

import io.github.braayy.column.RedbitColumn;
import io.github.braayy.struct.RedbitStruct;

public class UsersStruct extends RedbitStruct {

    @RedbitColumn(name = "uuid", sqlType = "VARCHAR", length = 36, idColumn = true)
    public String uuid;

    @RedbitColumn(sqlType = "VARCHAR", length = 16)
    public String name;

    @RedbitColumn(sqlType = "TINYINT")
    public Byte age;

}
