package com.vn.id.uuid;

import com.vn.distributed.id.base.AbstractIDGenerator;

import java.util.UUID;

public class UUIDGenerator extends AbstractIDGenerator {
    @Override
    public String generate() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
