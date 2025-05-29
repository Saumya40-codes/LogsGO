import { Autocomplete } from "@mantine/core";
import styles from "./expressionInput.module.css";

const ExpressionInput = () => {
    return (
        <Autocomplete
            placeholder="Enter expression"
            label="Expression"
            description="Get started with: your_service_name"
            data={[]} // TODO: Store all possible expressions in a constant and use it here on startup
            maxDropdownHeight={400}
            className={styles.expressionInput}
        />
    )
}

export default ExpressionInput;