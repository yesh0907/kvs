import { cleanEnv, makeValidator, str, EnvError } from "envalid";

const addressValidator = makeValidator(
  (input: string) => {
    const regex = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$/;
    if (!regex.test(input)) {
      throw new EnvError(`Invalid address format: ${input}`);
    }
    return input;
  });

const validateEnv = () => {
  cleanEnv(process.env, {
    NODE_ENV: str(),
    ADDRESS: addressValidator(),
  });
};

export default validateEnv;
