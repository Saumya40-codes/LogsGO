import ExpressionInput from "./ExpressionInput";
import styles from './dashboard.module.css';

const Dashboard = () => {
    return (
        <div className={styles.dashboard}>
            <div className={styles.dashboard__container}>
                <ExpressionInput />
            </div>
        </div>   
    )
}

export default Dashboard;