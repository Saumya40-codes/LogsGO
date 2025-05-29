import styles from './navbar.module.css';
import logo from "../../assets/logsGo_logo-nav.png";
import { Button } from '@mantine/core';

interface LinksProps {
    label: string;
    href: string;
}

const Navbar = () => {

    const links:LinksProps[] = [
        { label: 'Home', href: '#' },
        { label: 'Configuration', href: '#' },
        { label: 'S3 Bucket', href: '#' }
    ];

    return (
        <div className={styles.navbar}>
            <div className={styles.navbar__container}>
                <div className={styles.navbar__logo}>
                    <img src={logo} alt="LogsGo Logo" className={styles.navbar__logoImage} />
                    <span className={styles.navbar__logoText}>LogsGo</span>
                </div>
                <div className={styles.navbar__links}>
                    {links.map((link, index) => (
                        <Button
                            key={index}
                            variant="subtle"
                            component="a"
                            href={link.href}
                            className={styles.navbar__link}
                        >
                            {link.label}
                        </Button>
                    ))
                }
                </div>
            </div>
        </div>
    )
}

export default Navbar;