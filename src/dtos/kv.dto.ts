import { IsByteLength, ValidationArguments } from "class-validator";

const MB = 1024 * 1024;
export class UpdateKVDto {
    @IsByteLength(0, 8*MB, {
      message: (args: ValidationArguments): string => {
        if (typeof args.value === "string") {
            return "val too large";
        } else {
            return "bad request";
        }
      }
    })
    public val: string;
}
